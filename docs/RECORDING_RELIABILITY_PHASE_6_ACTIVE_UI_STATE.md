# Recording Reliability — Phase 6 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Active recording UI state model
Last updated: 2026-06-10

This document captures Phase 6 of the recording reliability audit: making the active recording UI reflect the true state of the logical recording session and the physical audio capture.

The key product rule:

> The UI must never tell the user that the app is recording when the physical capture is not actually validated and receiving audio buffers.

---

## Phase 6 Goal

Separate user-facing logical recording state from physical capture state in the UI.

The app can have a logical recording session alive while physical capture is temporarily unavailable.

Those states must look different.

---

## Core Distinction

### Logical recording is active

Meaning:

- user started a recording;
- a `KnowledgeItem` exists;
- previous audio may already be saved;
- Finish should remain available;
- route recovery may still happen;
- session has not been intentionally ended by the user.

### Physical capture is recording

Meaning:

- `AVAudioEngine` is running;
- input tap is installed;
- file writer has an open valid segment;
- first buffer after route start/restart has arrived;
- timer/audio meter represent actual capture progress.

These are not the same state.

---

## Current Observed UX Failures

### F-001 — UI appears to show Recording while timer is frozen

Observed:

```text
User starts recording.
Bluetooth route change occurs.
UI still suggests iPhone/recording-like state.
Timer stops around 15s/22s/25s.
Audio meter may not reflect input.
User cannot tell whether app is recording.
```

Problem:

The UI mixes logical recording alive with physical capture active.

Required:

When physical capture is not receiving buffers, UI must not show normal Recording.

---

### F-002 — Pause appears when there is nothing to pause

Observed:

```text
App is waiting for microphone input.
Physical capture is not active.
UI still exposes Pause or flickers Pause/Resume.
```

Problem:

Pause is only meaningful when physical capture is active.

Required:

When switching/validating/waiting, show Finish and recovery actions, not Pause.

---

### F-003 — Finish sometimes looked secondary or unavailable

Observed:

During switching/waiting states, the user needed to click quickly or was unsure whether Finish would work.

Problem:

Finish must be the permanent escape hatch for an active logical recording.

Required:

Finish must be visible, enabled, and reliable in every active logical state.

---

### F-004 — Error message says recording failed even when audio was preserved

Observed:

After route failure and Finish, app may show Recording Error even though some audio exists.

Problem:

This collapses different failure classes:

```text
no audio recorded
recording saved but route recovery failed
recording saved but transcript failed
recording artifacts inconsistent
```

Required:

If valid audio bytes exist, do not show generic Recording Error.

Use recoverable statuses:

```text
Audio saved. Transcript needs retry.
Audio saved. Microphone switch failed.
Audio saved. Processing issue detected.
```

---

## Required UI State Model

The UI should represent two dimensions:

```text
Logical session state
Physical capture state
```

### LogicalRecordingUIState

Suggested:

```swift
enum LogicalRecordingUIState: Equatable {
    case idle
    case active
    case stopping
    case stopped
    case failedNoAudio(String)
    case savedWithIssue(String)
}
```

### PhysicalCaptureUIState

Suggested:

```swift
enum PhysicalCaptureUIState: Equatable {
    case none
    case recording(inputName: String)
    case pausedByUser
    case switchingMicrophone(from: String?, to: String?)
    case validatingMicrophone(inputName: String?)
    case waitingForInput(reason: String, lastKnownInput: String?)
    case interruptedBySystem(reason: String?)
    case unavailable(reason: String)
}
```

### Combined model

The screen can then derive controls from both states.

For MVP, this can be represented as one enum, but it must not lose the distinction.

---

## Required Screen States

### State 1 — Recording normally

Condition:

```text
logical = active
physical = recording
first buffer validated
```

UI:

```text
Recording
Input: iPhone / Jabra / etc.
Timer advancing
Audio meter active
Buttons: Pause, Finish
```

---

### State 2 — Paused by user

Condition:

```text
logical = active
physical = pausedByUser
```

UI:

```text
Paused
Timer frozen
Input: last known input
Buttons: Resume, Finish
```

---

### State 3 — Switching microphone

Condition:

```text
logical = active
physical = switchingMicrophone
```

UI:

```text
Switching microphone...
Trying: iPhone / Jabra / Automatic
Timer frozen with label such as "Recording paused while switching"
Buttons: Finish
Optional: Use iPhone Microphone
```

Do not show Pause.

---

### State 4 — Validating microphone

Condition:

```text
engine may be running
first buffer has not arrived yet
```

UI:

```text
Checking microphone...
Trying: input name
Waiting for audio signal
Buttons: Finish
Optional: Use iPhone Microphone
```

Do not show Recording yet.

Do not resume timer.

---

### State 5 — Waiting for input

Condition:

```text
logical = active
physical = waitingForInput
```

UI:

```text
Microphone not available
Last saved audio is safe
Waiting for input...
Buttons: Finish, Try Again, Use iPhone Microphone
```

Do not show Pause.

Do not show normal Recording.

---

### State 6 — Stopping/finalizing

Condition:

```text
user tapped Finish
```

UI:

```text
Saving recording...
Preparing transcript...
```

Controls:

- disable duplicate Finish;
- do not allow route recovery UI to reappear;
- ignore late route recovery commits.

---

### State 7 — Saved with issue

Condition:

```text
valid audio exists
but route recovery / artifact validation / transcript failed
```

UI:

```text
Audio saved
Transcript needs retry
```

Actions:

- Retry transcript;
- Open recording detail;
- Export audio;
- View diagnostic if developer mode.

Do not say generic Recording Error.

---

### State 8 — Failed with no audio

Condition:

```text
no valid audio segment
no valid audio.m4a
recording could not be saved
```

UI:

```text
Recording failed
No audio was saved
```

This is the only case where a severe recording error is appropriate.

---

## Button Rules

### Pause

Show only when:

```text
physical capture state == recording
```

Do not show during:

- switching microphone;
- validating route;
- waiting for input;
- interrupted by system;
- stopping;
- saved with issue.

### Resume

Show only when:

```text
paused by user
```

Do not use Resume as generic route recovery.

Route recovery should be explicit:

```text
Try Again
Use iPhone Microphone
```

### Finish

Show when:

```text
logical session is active
```

That includes:

- recording;
- paused;
- switching;
- validating;
- waiting;
- interrupted.

Finish must always be enabled unless finalization is already running.

### Use iPhone Microphone

Show when:

- waiting for input;
- switching/validating external input longer than timeout;
- Bluetooth failed validation;
- built-in mic is available.

### Try Again

Show when:

- waiting for input;
- previous route recovery failed;
- no restart currently in progress.

---

## Timer Rules

The timer must distinguish:

### Captured audio duration

Duration of valid audio segments.

### Logical session elapsed time

Wall-clock time since user started recording.

### Paused/recovery time

Time when logical session was alive but physical capture was not recording.

Recommended UI:

```text
Recorded: 00:25
Paused while switching: 00:08
```

For MVP, the main timer should not continue advancing during switching/waiting if no audio is being captured.

But the UI should clearly say why it stopped.

---

## Audio Meter Rules

Show active meter only when physical capture is receiving buffers.

During validating:

```text
Checking signal...
```

During waiting:

```text
No microphone signal
```

Do not show stale audio levels from before route change.

Reset level to zero when physical capture stops.

---

## Source Label Rules

The input label must not imply capture success.

Examples:

Bad:

```text
Input: iPhone
```

while state is waiting and no buffer is arriving.

Better:

```text
Trying iPhone microphone...
No audio signal yet
```

or:

```text
Last input: iPhone
Waiting for microphone
```

### Required distinction

- selected/attempted input;
- last known input;
- validated recording input.

Only call it the active input after first buffer validation.

---

## Mapping from Capture State to UI

Suggested mapping:

```swift
switch captureState {
case .recording:
    ui = .recording(inputName: validatedInput)
case .pausedByUser:
    ui = .paused
case .reconfiguringRoute:
    ui = .switchingMicrophone
case .validatingRoute:
    ui = .validatingMicrophone
case .waitingForUsableInput:
    ui = .waitingForInput
case .interruptedBySystem:
    ui = .interrupted
case .failedFatal(let reason):
    if artifactValidator.hasValidAudio(itemId) {
        ui = .savedWithIssue(reason)
    } else {
        ui = .failedNoAudio(reason)
    }
case .stopped:
    ui = .finalizedOrNavigating
case .idle:
    ui = .idle
}
```

---

## HomeView / CaptureViewModel Requirements

The current home screen should not route all active states to the same visual panel without state-specific controls.

The recording panel may be shared, but it must render different content/buttons depending on state.

Required:

- one state-specific view model for title/subtitle;
- one state-specific button model;
- one state-specific timer/meter model;
- no direct button availability based only on coarse `recordingState`.

Suggested UI model:

```swift
struct ActiveRecordingPresentation {
    let title: String
    let subtitle: String
    let inputLabel: String?
    let timerLabel: String
    let meterMode: MeterMode
    let primaryAction: RecordingAction
    let secondaryActions: [RecordingAction]
    let isDestructiveError: Bool
}
```

---

## Error Messaging Rules

### Do not say

```text
Recording Error
```

when valid audio exists.

### Say instead

```text
Audio saved. Microphone switch failed.
```

or:

```text
Audio saved. Transcript needs retry.
```

or:

```text
Audio saved. Some segments need repair.
```

Only use `Recording failed` when no usable audio was saved.

---

## Required Diagnostics in UI Debug Mode

For developer/debug builds, active recording screen should optionally show:

```text
logical state
capture state
recordingIntent
routeRecoveryGeneration short id
isPhysicalRestartInProgress
isTapInstalled
engine.isRunning
lastBufferReceivedAt age
current input route
available inputs
current segment index/file
valid manifest segment count
```

This would make real-device tests much easier.

---

## Acceptance Tests

### Test 1 — Normal recording

Expected:

- UI says Recording;
- timer advances;
- meter responds;
- buttons: Pause, Finish.

---

### Test 2 — Bluetooth switch starts

Expected:

- UI says Switching microphone;
- timer stops or clearly marks switching pause;
- Pause disappears;
- Finish remains available.

---

### Test 3 — Validating route

Expected:

- UI says Checking microphone;
- no Recording label yet;
- no Pause;
- Finish available;
- if first buffer arrives, transition to Recording.

---

### Test 4 — Waiting for input

Expected:

- UI says Microphone not available / waiting for input;
- source label does not falsely imply active recording;
- buttons: Finish, Try Again, Use iPhone Microphone when available;
- no Pause.

---

### Test 5 — Finish during waiting

Expected:

- first tap works;
- UI transitions to Saving recording;
- no later route task returns UI to Recording;
- if audio exists, final state is saved or saved-with-issue, not generic Recording Error.

---

### Test 6 — Audio saved but transcript failed

Expected:

- UI says Audio saved / Transcript needs retry;
- user can open item;
- user can export audio;
- user can retry processing.

---

## Phase 6 Retest Gate

Do not polish Bluetooth behavior until:

- [ ] UI distinguishes logical active vs physical recording;
- [ ] Recording label only appears after first buffer validation;
- [ ] Pause appears only during real physical capture;
- [ ] Finish appears in every active logical state;
- [ ] source label distinguishes trying/last/validated input;
- [ ] generic Recording Error is not shown when audio exists;
- [ ] timer/meter do not imply fake recording.

---

## Summary

Most user confusion came from the UI compressing too many states into one recording panel.

The fix is not just a label change. The UI must model the real system:

```text
logical recording can be alive
while physical capture is unavailable
```

Once the UI respects that distinction, route-change failures become understandable and recoverable instead of appearing like silent recording loss.
