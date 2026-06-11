# Recording Reliability — Phase 2 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Crash-safe physical capture restart
Last updated: 2026-06-10

This document captures Phase 2 of the recording reliability audit: making microphone route changes crash-safe and deterministic.

Phase 2 must not optimize Bluetooth behavior yet. The goal is narrower:

> A physical capture restart must never crash the app, corrupt the writer, race with Finish, or transition the UI back to Recording unless real audio is flowing.

---

## Phase 2 Goal

Make route change handling safe even when iOS audio hardware is unstable.

A route change must behave like a transaction:

```text
checkpoint current segment
release old physical capture
wait for route settle
attempt fresh physical capture
validate first buffer
commit recording state
```

If any step fails:

```text
preserve already-saved segments
enter waiting state
keep Finish available
```

---

## Finding 1 — A Boolean guard is not enough unless every restart path is routed through it

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Current direction

The service now has restart-safety fields such as:

- `isPhysicalRestartInProgress`
- `physicalRestartTask`
- `routeRecoveryGeneration`
- `routeChangeTask`
- `waitingInputProbeTask`
- `isTapInstalled`

This is good direction.

### Problem

A Boolean guard only works if every physical capture operation is forced through one gate.

The following operations must never overlap:

- route change recovery,
- waiting input probe recovery,
- force built-in mic recovery,
- manual resume,
- user pause,
- user Finish/Stop,
- engine teardown,
- engine startup,
- tap remove/install,
- writer segment close/open.

If any path bypasses the guard, the app can still crash or corrupt state.

### Required rule

Create a single physical restart gate:

```swift
private func runPhysicalRestart(
    reason: String,
    operation: @MainActor @Sendable () async -> Void
) async
```

or use an actor / serial operation queue.

This gate must:

- check if restart is already running;
- set `isPhysicalRestartInProgress = true`;
- create/update generation token;
- run the operation;
- use `defer` to reset the flag;
- respect cancellation;
- refuse to commit if user already pressed Finish.

### Acceptance

- Waiting probe cannot start recovery while route-change recovery is running.
- Route-change recovery cannot start while built-in mic recovery is running.
- Finish cancels current restart and prevents it from committing.

---

## Finding 2 — Finish/Stop must be the highest-priority operation

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Problem

During route change, there may be pending async work:

- debounce task,
- waiting probe task,
- physical restart task,
- validation timeout,
- engine start retry,
- route generation commit.

If the user taps Finish while any of these are alive, they must not later transition the service back to Recording.

### Required rule

`stopRecording()` / force finish must perform this first:

```swift
recordingIntent = .userStopped
routeRecoveryGeneration = UUID()
routeChangeTask?.cancel()
waitingInputProbeTask?.cancel()
physicalRestartTask?.cancel()
```

Then it must safely tear down physical capture:

```swift
safelyRemoveTap(reason: "finish")
engine.stop()
engine.reset()
fileWriter.finishRecording()
try? sessionManager.deactivate()
transition(to: .stopped, reason: "finish")
```

No later task may commit `.recording` after this.

### Acceptance

- Tap Finish during `switching microphone`.
- App finalizes once.
- No route recovery logs after finalization commit `.recording`.
- UI does not return to recording after Finish.

---

## Finding 3 — `commitRecoveredRouteToRecording` must validate first buffer, not only engine state

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Problem

The service tracks `lastBufferReceivedAt`, which is good. But the commit gate must explicitly validate that an audio buffer arrived after this restart attempt.

`engine.isRunning` is not enough.

An engine can be running while the input route is not delivering audio. This matches observed behavior:

```text
UI shows iPhone or Recording-related state
counter stops or resumes incorrectly
microphone not delivering audio
```

### Required fix

Before starting a physical restart, record a validation start timestamp:

```swift
let validationStartedAt = Date()
lastBufferReceivedAt = .distantPast
```

The tap callback must update `lastBufferReceivedAt` on any received buffer.

Commit only if:

```swift
engine.isRunning
recordingIntent == .userWantsRecording
generation == routeRecoveryGeneration
lastBufferReceivedAt >= validationStartedAt
```

Optionally also require a minimum RMS / non-zero frame length, but first-buffer timestamp is the minimum requirement.

### Acceptance

- Engine starts but no input buffers arrive.
- Service must stay in `waitingForUsableInput` or `validatingRoute` timeout.
- UI must not show Recording.
- Timer must not resume.

---

## Finding 4 — New segment should not be committed to manifest until route validates

### Files

- `wawa-note/Audio/AudioCaptureService.swift`
- `wawa-note/Connectivity/RecordingCoordinator.swift`
- `wawa-note/Audio/AudioFileWriter.swift`

### Problem

A physical restart may open a new file before knowing whether the route will deliver audio.

If the route fails, this can create:

- zero-byte segments,
- manifest entries for failed attempts,
- misleading concat input,
- confusing debug reports.

### Required rule

The new physical segment should be provisional until first buffer validation.

Two acceptable designs:

### Option A — Open early, append after validation

- Open file before engine start.
- Do not append `RecordingSegment` to manifest yet.
- If first buffer arrives, append segment to manifest.
- If validation fails, close/delete zero-byte file or mark it as failed but exclude from manifest.

### Option B — Append immediately but mark as provisional

Add state to `RecordingSegment`, e.g.:

```swift
enum SegmentStatus: Codable {
    case provisional
    case valid
    case failed
}
```

Only valid segments are eligible for concat/transcription.

### Recommendation

For MVP, use Option A. Keep the manifest clean: only valid or previously checkpointed segments should appear.

### Acceptance

- Failed route attempt does not create a valid manifest segment.
- Zero-byte segment files are removed or clearly reported as orphaned.
- Concat ignores failed provisional attempts but reports them in diagnostics.

---

## Finding 5 — Old engine teardown and new engine startup must be separate phases

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Problem

Route change is hardware-unstable. Accessing `engine.inputNode`, input format, or installing taps too early can crash or produce invalid formats.

### Required physical restart sequence

Use strict phases:

### Phase A — checkpoint and teardown

```text
stop accepting writes
checkpoint current segment
safelyRemoveTap
engine.stop
engine.reset
try? sessionManager.deactivate
```

### Phase B — route settle

```text
sleep 500–1000ms
longer if Bluetooth involved
```

### Phase C — fresh startup

```text
engine = AVAudioEngine()
try sessionManager.configureForRecording()
validate currentRoute / availableInputs
validate sampleRate > 0
validate input channel count > 0
create AVAudioFormat
open provisional next segment
install tap
engine.prepare
engine.start
wait for first buffer
commit segment + recording state
```

Do not interleave old-engine operations with new-engine startup.

### Acceptance

- No `inputNode` access before route settle and session activation.
- New route gets a new engine instance.
- Failure in startup leaves old checkpointed segment safe.

---

## Finding 6 — `safelyRemoveTap` and `safelyInstallTap` must be the only tap API paths

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Problem

The service has `isTapInstalled`, which is the right direction. But the invariant must be enforced: no direct `engine.inputNode.removeTap(...)` or direct install call outside safe wrappers.

### Required rule

Only these methods may touch tap installation:

```swift
private func safelyInstallTap(reason: String) throws
private func safelyRemoveTap(reason: String)
```

`safelyInstallTap` must:

- remove existing tap if `isTapInstalled` is true;
- validate input format;
- install tap;
- set `isTapInstalled = true` only after success.

`safelyRemoveTap` must:

- no-op if `isTapInstalled == false`;
- remove tap inside safe boundary;
- set `isTapInstalled = false` even if removal throws/fails where possible.

### Acceptance

- Search codebase for `removeTap` and `installTap` usage.
- All usages go through safe wrappers.

---

## Finding 7 — Waiting probe must not be both observer and actor

### File

`wawa-note/Audio/AudioCaptureService.swift`

### Problem

The waiting probe exists to prevent `waitingForUsableInput` from becoming a black hole. That is good.

But the probe should not perform recovery directly unless it goes through the physical restart gate.

### Required rule

The probe may observe and request recovery. It must not bypass the single-flight gate.

Recommended pattern:

```swift
if hasBuiltInMic {
    await requestPhysicalRestart(reason: "waitingProbeBuiltInMic")
}
```

where `requestPhysicalRestart` is the same path used by route change and manual resume.

### Acceptance

- Probe cannot race with route change handler.
- Probe cannot reopen a segment while route handler is already opening one.
- Probe stops when user pauses or finishes.

---

## Finding 8 — Route restart must have structured diagnostics

### Files

- `AudioCaptureService.swift`
- `AudioSessionManager.swift`

### Problem

Current logs are useful but not structured enough to reconstruct failures from real-device tests.

### Required diagnostic object

Create a route restart report:

```swift
struct PhysicalRestartReport {
    let reason: String
    let generation: UUID
    let startedAt: Date
    let previousState: AudioCaptureState
    let checkpointedSegment: ClosedSegmentInfo?
    let routeBefore: AudioRouteSnapshot
    let routeAfterSettle: AudioRouteSnapshot
    let selectedInput: String?
    let sampleRate: Double
    let channelCount: Int
    let didInstallTap: Bool
    let didStartEngine: Bool
    let didReceiveFirstBuffer: Bool
    let openedSegmentIndex: Int?
    let openedSegmentFileName: String?
    let result: ResultKind
    let errorDescription: String?
}
```

Minimum result kinds:

```swift
enum ResultKind {
    case resumed
    case waitingForInput
    case cancelledByUser
    case failedButAudioPreserved
}
```

### Acceptance

Every route restart attempt logs:

- why it started,
- what segment was checkpointed,
- which input was attempted,
- whether engine started,
- whether first buffer arrived,
- whether the segment was committed.

---

## Phase 2 Acceptance Test

Manual test after Phase 2 fixes:

```text
Start recording on iPhone.
Speak "part one".
Insert Jabra Bluetooth earbud.
Observe switching.
Tap Finish during switching or waiting.
```

Expected:

- no crash;
- Finish responds immediately;
- old segment remains valid;
- no async route task resumes recording after Finish;
- if restart fails, app enters waiting instead of crashing;
- if engine starts without buffers, UI does not show Recording;
- transcript or artifact pipeline preserves pre-switch audio.

---

## Recommended Claude Code Task

Focus only on Phase 2.

Do not optimize Bluetooth selection yet.

Priority order:

1. Build/enforce a single physical restart gate.
2. Make Finish cancel every route/restart/probe/validation task before teardown.
3. Ensure commit-to-recording requires first buffer after validation start.
4. Make new segment provisional until route validates.
5. Enforce safe tap wrappers as the only tap API path.
6. Split restart into teardown, settle, startup, validate, commit.
7. Add structured route restart diagnostics.

---

## Retest Gate

Do not continue Bluetooth UX tuning until these pass:

- [ ] route change does not crash;
- [ ] Finish during switching works;
- [ ] no restart task can commit after Finish;
- [ ] Recording state requires first buffer;
- [ ] failed route startup does not create valid manifest segment;
- [ ] previous audio segment remains valid.
