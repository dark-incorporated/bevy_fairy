//! **`bevy_fairy/input` over a real socket, read by an ordinary `Update` system** (issue #16).
//!
//! `plugin_input_edges.rs` queues [`PendingInput`](bevy_fairy::PendingInput) directly, and
//! `plugin_input_params.rs` runs the handler with `run_system_once_with`. Neither crosses the route a
//! caller takes: JSON-RPC over HTTP into a running app, answered in `RemoteLast`, applied in the next
//! frame's `PreUpdate`. These tests do. Each one starts an app with `RemoteHttpPlugin` on a free port,
//! calls the method from the test's thread the way an agent's client does, and reads a log that an
//! `Update` system writes on every frame: the keys `ButtonInput<KeyCode>` called just pressed,
//! pressed and just released on that frame.
//!
//! # What #16 measured
//!
//! A host pinned to `06c9c18` (v0.3.1) sent
//! `{"kind":"Keyboard","key":"Digit7","modifiers":["ControlLeft"],"action":"Tap"}` ten times. Each
//! reply was `success: true`, and its `Ctrl+7` never fired. The same chord sent as `Press` and
//! `Release` calls fired first time. Every `Tap` the host measured carried `modifiers`, a field this
//! method has never read: at `06c9c18` `InputCommand` had no `deny_unknown_fields`, so the field was
//! dropped and the call tapped a bare `7` (B-121, refused since 0.12.0).
//!
//! The tests below hold the three facts that separate a broken tap from a dropped modifier, on the
//! route a caller uses: a tap is `just_pressed` on exactly one frame, a chord tapped under a held
//! modifier fires like the chord sent key by key, and the measured call is refused and moves no key.
//! The guide's own calls are read out of `docs/guide/input.md` and sent as written, so the documented
//! form is the tested form.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bevy::app::ScheduleRunnerPlugin;
use bevy::input::InputPlugin;
use bevy::prelude::*;
use bevy::remote::error_codes;
use bevy::remote::http::RemoteHttpPlugin;
use bevy_fairy::DebuggerPlugin;
use serde_json::{Value, json};

/// The guide whose calls [`the_guides_tap_is_a_tap`] and [`the_guides_chord_fires`] send.
const GUIDE: &str = include_str!("../docs/guide/input.md");

/// How long any one wait may take before the test says what it was waiting for.
const PATIENCE: Duration = Duration::from_secs(60);

/// Frames to let run after a reply before the log is read: the press lands on the first frame after
/// the reply and a tap's release on the second, so six leave room without hiding a late edge.
const SETTLE_FRAMES: usize = 6;

/// What `ButtonInput<KeyCode>` said on one frame, as an `Update` system read it.
#[derive(Clone, Debug, Default)]
struct Frame {
    just_pressed: Vec<KeyCode>,
    pressed: Vec<KeyCode>,
    just_released: Vec<KeyCode>,
}

/// Every frame the app has run, shared with the test's thread.
#[derive(Resource, Clone, Default)]
struct Frames(Arc<Mutex<Vec<Frame>>>);

impl Frames {
    fn lock(&self) -> MutexGuard<'_, Vec<Frame>> {
        // A panic while the log was held cannot leave a half-written frame: the push is the whole write.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The reader every host has: an ordinary system in `Update` looking at `ButtonInput<KeyCode>`.
fn record(keys: Res<ButtonInput<KeyCode>>, frames: Res<Frames>) {
    let frame = Frame {
        just_pressed: keys.get_just_pressed().copied().collect(),
        pressed: keys.get_pressed().copied().collect(),
        just_released: keys.get_just_released().copied().collect(),
    };
    frames.lock().push(frame);
}

/// A port that was free a moment ago; see `fixture_ground_truth.rs` for the race this leaves open.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the ephemeral port")
        .port()
}

/// A running game with the plugin and the HTTP transport, and the log its `Update` system writes.
struct Game {
    port: u16,
    frames: Frames,
}

impl Game {
    /// Start the game on its own thread, at 120 frames a second, and wait until it answers.
    fn start() -> Self {
        let port = free_port();
        let frames = Frames::default();
        let log = frames.clone();
        std::thread::spawn(move || {
            App::new()
                .add_plugins(MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(
                    Duration::from_secs_f64(1.0 / 120.0),
                )))
                .add_plugins(InputPlugin)
                // Owns the BRP RemotePlugin; the HTTP transport is added next to it, as a host does.
                .add_plugins(DebuggerPlugin::default())
                .add_plugins(RemoteHttpPlugin::default().with_port(port))
                .insert_resource(log)
                .add_systems(Update, record)
                .run();
        });
        let game = Self { port, frames };
        let deadline = Instant::now() + PATIENCE;
        while let Err(last) = game.try_call("bevy_fairy/status", json!({})) {
            assert!(
                Instant::now() < deadline,
                "the game on port {port} never answered `bevy_fairy/status`; last: {last}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        game
    }

    /// One JSON-RPC call over HTTP/1.1, the way an agent's client sends it.
    fn try_call(&self, method: &str, params: Value) -> Result<Value, String> {
        let body =
            json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port))
            .map_err(|e| format!("connect to {}: {e}", self.port))?;
        stream
            .set_read_timeout(Some(PATIENCE))
            .map_err(|e| format!("set a read timeout: {e}"))?;
        let request = format!(
            "POST / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
             Content-Length: {length}\r\nConnection: close\r\n\r\n{body}",
            port = self.port,
            length = body.len(),
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("send the request: {e}"))?;
        let mut reply = String::new();
        stream
            .read_to_string(&mut reply)
            .map_err(|e| format!("read the reply: {e}"))?;
        let (head, payload) = reply
            .split_once("\r\n\r\n")
            .ok_or_else(|| format!("no HTTP body in {reply:?}"))?;
        // `bevy_remote`'s body has no size hint, so hyper sends a 1.1 reply chunked.
        let json_body = if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            dechunk(payload).ok_or_else(|| format!("a malformed chunked body: {payload:?}"))?
        } else {
            payload.to_owned()
        };
        serde_json::from_str(&json_body).map_err(|e| format!("non-JSON reply ({e}): {reply:?}"))
    }

    fn call(&self, method: &str, params: Value) -> Value {
        self.try_call(method, params.clone())
            .unwrap_or_else(|e| panic!("no JSON-RPC reply to `{method}` {params}: {e}"))
    }

    /// Send one `bevy_fairy/input` call and require it accepted.
    fn input(&self, params: Value) {
        let reply = self.call("bevy_fairy/input", params.clone());
        assert_eq!(
            reply["result"]["success"],
            json!(true),
            "`bevy_fairy/input` {params} must be accepted; the reply was {reply}"
        );
    }

    fn frame_count(&self) -> usize {
        self.frames.lock().len()
    }

    /// The frames from `start` on, once [`SETTLE_FRAMES`] more have run past the newest one now.
    fn frames_since(&self, start: usize) -> Vec<Frame> {
        let target = self.frame_count() + SETTLE_FRAMES;
        let deadline = Instant::now() + PATIENCE;
        while self.frame_count() < target {
            assert!(
                Instant::now() < deadline,
                "the game stopped running frames at {}",
                self.frame_count()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        self.frames.lock()[start..].to_vec()
    }
}

/// The payload of a chunked HTTP/1.1 body: each chunk is its length in hex, CRLF, the bytes, CRLF,
/// and a zero-length chunk ends it.
fn dechunk(mut rest: &str) -> Option<String> {
    let mut out = String::new();
    loop {
        let (size, tail) = rest.split_once("\r\n")?;
        let size = usize::from_str_radix(size.split(';').next()?.trim(), 16).ok()?;
        if size == 0 {
            return Some(out);
        }
        out.push_str(tail.get(..size)?);
        rest = tail.get(size..)?.strip_prefix("\r\n")?;
    }
}

/// **A tap, as a reader of `ButtonInput` sees one**: `just_pressed` on exactly one frame, `pressed`
/// on that frame only, and `just_released` on the frame after it.
fn assert_one_tap(frames: &[Frame], key: KeyCode, sent: &str) {
    let pressed_on: Vec<usize> = frames
        .iter()
        .enumerate()
        .filter(|(_, frame)| frame.just_pressed.contains(&key))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        pressed_on.len(),
        1,
        "{sent} was answered success, so `just_pressed({key:?})` must be true on exactly one frame; \
         it was on frames {pressed_on:?} of {frames:#?}"
    );
    let at = pressed_on[0];
    let held_on: Vec<usize> = frames
        .iter()
        .enumerate()
        .filter(|(_, frame)| frame.pressed.contains(&key))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        held_on,
        vec![at],
        "{sent}: `pressed({key:?})` must be true on the press frame only, as a physical tap is; \
         frames: {frames:#?}"
    );
    let after = frames
        .get(at + 1)
        .unwrap_or_else(|| panic!("{sent}: no frame ran after the press; frames: {frames:#?}"));
    assert!(
        after.just_released.contains(&key),
        "{sent}: the frame after the press must release {key:?}; frames: {frames:#?}"
    );
}

/// **The frames on which a host's chord reader fires**: the modifier held while the key goes down,
/// the shape of every `pressed(modifier) && just_pressed(key)` binding.
fn chord_frames(frames: &[Frame], modifier: KeyCode, key: KeyCode) -> Vec<usize> {
    frames
        .iter()
        .enumerate()
        .filter(|(_, frame)| frame.pressed.contains(&modifier) && frame.just_pressed.contains(&key))
        .map(|(index, _)| index)
        .collect()
}

/// The calls of the first ```` ```jsonc ```` block under `heading` in the guide, as a caller copies
/// them: one JSON object per line, a `//` comment after it.
fn guide_calls(heading: &str) -> Vec<Value> {
    let (_, section) = GUIDE
        .split_once(heading)
        .unwrap_or_else(|| panic!("docs/guide/input.md has no `{heading}`"));
    let block = section
        .split_once("```jsonc")
        .and_then(|(_, rest)| rest.split_once("```"))
        .map(|(block, _)| block)
        .unwrap_or_else(|| panic!("`{heading}` in docs/guide/input.md has no ```jsonc block"));
    block
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(call, _)| call).trim())
        .filter(|call| !call.is_empty())
        .map(|call| {
            serde_json::from_str(call)
                .unwrap_or_else(|e| panic!("the guide's call `{call}` is not JSON: {e}"))
        })
        .collect()
}

fn key_of(call: &Value) -> KeyCode {
    serde_json::from_value(call["key"].clone())
        .unwrap_or_else(|e| panic!("the guide's call {call} names no KeyCode: {e}"))
}

/// **The reported call, without the field it could not have honoured.** `KeyQ` with `action: "Tap"`
/// crosses the socket, is applied on the next `PreUpdate`, and an `Update` system sees one frame of
/// `just_pressed`; the call with no `action` is the same tap.
#[test]
fn a_tap_over_http_is_just_pressed_on_exactly_one_frame() {
    let game = Game::start();
    for params in [
        json!({"kind": "Keyboard", "key": "KeyQ", "action": "Tap"}),
        json!({"kind": "Keyboard", "key": "KeyQ"}),
    ] {
        let start = game.frame_count();
        game.input(params.clone());
        assert_one_tap(
            &game.frames_since(start),
            KeyCode::KeyQ,
            &params.to_string(),
        );
    }
}

/// **A chord is the modifier held around a tap**, and it fires on the same frame shape as the chord
/// sent key by key: one frame on which `ControlLeft` is pressed and `Digit7` is just pressed, and
/// nothing held once the calls are done.
#[test]
fn a_chord_tapped_under_a_held_modifier_fires_like_the_explicit_one() {
    let game = Game::start();
    let explicit = [
        json!({"kind": "Keyboard", "key": "ControlLeft", "action": "Press"}),
        json!({"kind": "Keyboard", "key": "Digit7", "action": "Press"}),
        json!({"kind": "Keyboard", "key": "Digit7", "action": "Release"}),
        json!({"kind": "Keyboard", "key": "ControlLeft", "action": "Release"}),
    ];
    let tapped = [
        json!({"kind": "Keyboard", "key": "ControlLeft", "action": "Press"}),
        json!({"kind": "Keyboard", "key": "Digit7", "action": "Tap"}),
        json!({"kind": "Keyboard", "key": "ControlLeft", "action": "Release"}),
    ];
    let mut fired = Vec::new();
    for (form, calls) in [
        ("pressed and released", &explicit[..]),
        ("tapped", &tapped[..]),
    ] {
        let start = game.frame_count();
        for params in calls {
            game.input(params.clone());
        }
        let frames = game.frames_since(start);
        let last = frames.last().expect("frames ran after the calls");
        assert!(
            last.pressed.is_empty(),
            "the chord {form} must leave nothing held, but {:?} is; frames: {frames:#?}",
            last.pressed
        );
        fired.push((
            form,
            chord_frames(&frames, KeyCode::ControlLeft, KeyCode::Digit7).len(),
        ));
    }
    assert_eq!(
        fired,
        vec![("pressed and released", 1), ("tapped", 1)],
        "Ctrl+7 tapped under a held Ctrl must fire exactly as often as Ctrl+7 pressed and released \
         key by key: once each"
    );
}

/// **The call #16 measured is refused by name, and nothing is tapped.** `modifiers` is not a field
/// of this method: answering it `success: true` while tapping the bare key is what made a working
/// tap look broken. The reply is a JSON-RPC error naming the field, and no frame shows `Digit7`.
#[test]
fn a_tap_with_modifiers_is_refused_and_taps_nothing() {
    let game = Game::start();
    for params in [
        json!({"kind": "Keyboard", "key": "Digit7", "modifiers": ["ControlLeft"], "action": "Tap"}),
        json!({"kind": "Keyboard", "key": "Digit7", "modifiers": ["ControlLeft"]}),
    ] {
        let start = game.frame_count();
        let reply = game.call("bevy_fairy/input", params.clone());
        let frames = game.frames_since(start);
        assert_eq!(
            reply["error"]["code"],
            json!(error_codes::INVALID_PARAMS),
            "{params} must be refused, not answered {reply}; frames after it: {frames:#?}"
        );
        let message = reply["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("modifiers"),
            "the refusal must name the field it refused, said: {message}"
        );
        assert!(
            frames
                .iter()
                .all(|frame| !frame.pressed.contains(&KeyCode::Digit7)
                    && !frame.just_released.contains(&KeyCode::Digit7)),
            "a refused call must deliver nothing, yet Digit7 moved: {frames:#?}"
        );
    }
}

/// **The guide's tap, sent as the guide writes it.** Every `Keyboard` call with a `key` in the
/// guide's first block is a tap, and each one is checked to be one.
#[test]
fn the_guides_tap_is_a_tap() {
    let game = Game::start();
    let taps: Vec<Value> = guide_calls("# Injecting input")
        .into_iter()
        .filter(|call| call["kind"] == json!("Keyboard") && call.get("key").is_some())
        .collect();
    assert!(
        !taps.is_empty(),
        "docs/guide/input.md's first block shows no key tap, so this test would check nothing"
    );
    for call in taps {
        let key = key_of(&call);
        let start = game.frame_count();
        game.input(call.clone());
        assert_one_tap(
            &game.frames_since(start),
            key,
            &format!("the guide's {call}"),
        );
    }
}

/// **The guide's chord, sent as the guide writes it**, fires the chord it names exactly once and
/// leaves nothing held.
#[test]
fn the_guides_chord_fires() {
    let game = Game::start();
    let calls = guide_calls("## Chords");
    let modifier = calls
        .iter()
        .find(|call| call["action"] == json!("Press"))
        .map(key_of)
        .expect("the guide's chord presses its modifier");
    let key = calls
        .iter()
        .find(|call| call.get("action").is_none() || call["action"] == json!("Tap"))
        .map(key_of)
        .expect("the guide's chord taps its key");
    let start = game.frame_count();
    for call in &calls {
        game.input(call.clone());
    }
    let frames = game.frames_since(start);
    assert_eq!(
        chord_frames(&frames, modifier, key).len(),
        1,
        "the guide's chord must fire {modifier:?}+{key:?} once; frames: {frames:#?}"
    );
    let last = frames.last().expect("frames ran after the calls");
    assert!(
        last.pressed.is_empty(),
        "the guide's chord must leave nothing held, but {:?} is",
        last.pressed
    );
}
