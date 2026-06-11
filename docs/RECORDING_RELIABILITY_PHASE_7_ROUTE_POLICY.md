# Recording Reliability — Phase 7 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Route switching product policy
Last updated: 2026-06-10

This document captures Phase 7 of the recording reliability audit: defining the product policy for microphone route switching.

The key product rule:

> Route switching should be conservative, bounded, explainable, and subordinate to audio preservation.

The goal is not to make every Bluetooth transition seamless. The goal is to make every transition safe.

---

## Phase 7 Goal

Define what the app should do when audio routes change during an active logical recording.

The product should choose reliability over cleverness.

A few seconds of missed audio during a route switch is acceptable.

Losing previously recorded audio is not acceptable.

Crashing is not acceptable.

Getting stuck in an endless retry loop is not acceptable.

Showing fake Recording is not acceptable.

---

## Guiding Principles

### Principle 1 — Preserve first, recover second

Before any route switch attempt:

```text
checkpoint current segment
persist manifest
verify file exists and has bytes
```

Only then attempt recovery.

---

### Principle 2 — Route recovery is best-effort

Bluetooth, headset, CarPlay, and USB behavior can be unstable.

The app should try reasonable recovery, but it must not pretend recovery is guaranteed.

If recovery fails:

```text
keep logical recording alive
show waiting state
keep Finish available
preserve existing audio
```

---

### Principle 3 — Built-in mic is the safe fallback

For MVP, iPhone built-in mic should be treated as the most reliable fallback.

If Bluetooth HFP fails validation, fall back to built-in mic instead of retrying Bluetooth indefinitely.

---

### Principle 4 — No infinite retry loops

Every route attempt must have:

- maximum attempt count;
- timeout;
- visible state;
- final waiting state;
- user escape hatch.

---

### Principle 5 — Validation beats route labels

A route label like `iPhone`, `Jabra`, or `Bluetooth Headset` does not mean audio is being captured.

A route is considered active only after first buffer validation.

---

## Recommended MVP Policy

### On route change while recording

1. Freeze physical recording UI.
2. Show `Switching microphone...`.
3. Checkpoint current segment.
4. Stop/release old physical capture.
5. Wait for audio route to settle.
6. Inspect available/current inputs.
7. Choose a candidate input.
8. Start fresh physical capture.
9. Wait for first buffer.
10. If validated, resume Recording UI.
11. If failed, fallback to built-in mic if available.
12. If fallback validates, resume Recording UI on iPhone.
13. If fallback fails, enter Waiting for Input.
14. Keep Finish available throughout.

---

## Input Selection Policy

### Priority during initial recording start

When the user first taps Record, it is acceptable to use the best available input:

1. Bluetooth HFP headset if already active and usable.
2. Wired headset / USB mic.
3. Built-in mic.

### Priority during mid-recording route change

During an active recording, safety should dominate.

Recommended priority:

1. If current route is clearly usable and validates quickly, use it.
2. If Bluetooth appears, try it once.
3. If Bluetooth fails first-buffer validation, quarantine that route temporarily.
4. Try built-in mic.
5. If built-in mic validates, continue recording on iPhone.
6. If no input validates, wait.

---

## Bluetooth Policy

### Bluetooth may be attempted when

- Bluetooth HFP is available;
- logical recording is active;
- no other physical restart is running;
- route is not quarantined;
- user has not paused/stopped;
- current segment has already been checkpointed.

### Bluetooth must be considered failed when

- engine cannot start;
- input format is invalid;
- channel count is zero;
- no first buffer arrives before timeout;
- route changes away during validation;
- same route failed recently and remains quarantined.

### After Bluetooth failure

Do not keep retrying Bluetooth immediately.

Instead:

```text
mark Bluetooth route temporarily quarantined
attempt built-in mic fallback
```

### Quarantine duration

For MVP:

```text
30–60 seconds or until explicit user action
```

Explicit user action may be:

```text
Try Bluetooth Again
```

---

## Built-in Mic Fallback Policy

Built-in mic fallback should run when:

- Bluetooth validation fails;
- Bluetooth disappears;
- app is in waiting state and built-in mic becomes available;
- user taps `Use iPhone Microphone`;
- route recovery times out.

Fallback must still go through the same physical restart transaction:

```text
release old capture
configure session
prefer built-in mic
fresh engine
new segment
first buffer validation
commit Recording
```

Do not simply change the label to iPhone.

---

## Waiting State Policy

`waitingForUsableInput` is not failure.

It means:

```text
logical recording is alive
no validated physical input is currently recording
previous audio is preserved
user can finish safely
```

### Waiting screen should show

```text
Microphone not available
Your previous audio is saved
Waiting for input...
```

Buttons:

- Finish;
- Use iPhone Microphone, if available;
- Try Again;
- Try Bluetooth Again, only if Bluetooth is available and not quarantined or user overrides quarantine.

### Waiting probe behavior

The probe may periodically check for built-in mic or a usable input, but it must not silently spin forever.

Recommended:

- probe every 1–2 seconds;
- no more than 3 automatic recovery attempts;
- after that, remain stable waiting;
- let user choose action.

---

## User Controls Policy

### Finish

Always available while logical recording is active.

Highest-priority action.

Cancels:

- route recovery;
- waiting probe;
- validation;
- engine retry;
- pending commit.

### Use iPhone Microphone

Available when:

- built-in mic exists;
- physical capture is not currently validated;
- app is switching/validating/waiting.

Action:

- cancels current route recovery;
- quarantines current failed external route for this session;
- attempts built-in mic physical restart.

### Try Again

Available when:

- waiting;
- no restart currently running.

Action:

- re-evaluates current route and attempts best safe input.

### Try Bluetooth Again

Available when:

- Bluetooth HFP exists;
- previous Bluetooth attempt failed;
- user explicitly chooses retry.

Action:

- clears Bluetooth quarantine for one attempt;
- tries Bluetooth once;
- if it fails again, returns to waiting/fallback.

---

## Error and Status Policy

### Do not show

```text
Recording Error
```

for ordinary route-switch failures if valid audio was saved.

### Show instead

```text
Audio saved. Microphone switch failed.
```

or:

```text
Still recording session. Waiting for microphone.
```

or:

```text
Audio saved. Transcript will continue from saved audio.
```

### Severe error only when

- storage write failed;
- microphone permission denied at start;
- no valid audio was captured;
- unrecoverable artifact corruption with no valid segment bytes.

---

## Route Recovery State Machine

Recommended route policy state machine:

```text
recording
  -> routeChangeDetected
  -> checkpointingCurrentSegment
  -> releasingOldCapture
  -> routeSettling
  -> selectingCandidateInput
  -> startingFreshCapture
  -> validatingFirstBuffer
      -> recording                      if validation succeeds
      -> tryingBuiltInFallback           if external input fails and built-in exists
      -> waitingForInput                 if no input validates
      -> stopped                         if user taps Finish
```

Every arrow must respect:

```text
if recordingIntent == userStopped: abort
if generation mismatch: abort
if userPaused: do not auto-resume
```

---

## Policy for User-Paused Recordings

If the user explicitly paused:

- route changes should not auto-resume recording;
- app may update available input information;
- Resume remains user-controlled;
- route recovery should not restart physical capture until user resumes.

User intent wins over route recovery.

---

## Policy for System Interruptions

System interruption is different from route change.

Examples:

- phone call;
- Siri;
- alarm;
- OS interruption.

Policy:

- save/checkpoint current segment if needed;
- move to interrupted state;
- do not confuse interruption with Bluetooth waiting;
- auto-resume only if user was recording before interruption and OS allows reactivation;
- otherwise show Resume/Finish.

---

## Telemetry / Debug Policy

For every route attempt, log:

```text
route policy decision
candidate input
why candidate was selected
whether Bluetooth was quarantined
whether built-in fallback was attempted
first buffer validation result
final route policy outcome
```

Required outcome labels:

```text
resumedOriginalRoute
resumedBluetooth
resumedBuiltInMic
waitingNoInput
waitingBluetoothFailed
cancelledByFinish
cancelledByPause
failedButAudioPreserved
```

---

## Acceptance Tests

### Test 1 — Bluetooth appears and validates

Setup:

```text
start on iPhone
insert Bluetooth HFP
Bluetooth delivers first buffer
```

Expected:

- current segment checkpointed;
- Bluetooth attempted once;
- first buffer validates;
- new segment committed;
- UI returns to Recording with Bluetooth input;
- previous segment preserved.

---

### Test 2 — Bluetooth appears and fails

Setup:

```text
start on iPhone
insert Bluetooth HFP
Bluetooth engine starts but no buffer arrives
built-in mic available
```

Expected:

- current segment checkpointed;
- Bluetooth attempt times out;
- Bluetooth quarantined;
- built-in mic fallback attempted;
- if built-in mic validates, Recording resumes on iPhone;
- no fake Recording during validation.

---

### Test 3 — Bluetooth fails and built-in also unavailable

Expected:

- app enters Waiting for Input;
- previous audio saved;
- Finish available;
- no infinite retry loop;
- UI explains state.

---

### Test 4 — User taps Finish during route switch

Expected:

- route recovery cancels;
- no later task resumes Recording;
- valid previous audio preserved;
- item finalizes or saved-with-issue.

---

### Test 5 — User paused, then route changes

Expected:

- app remains paused;
- no auto-resume;
- user controls Resume/Finish.

---

### Test 6 — Bluetooth failed once, user explicitly retries

Expected:

- one explicit Bluetooth retry happens;
- if it fails again, app returns to waiting or built-in fallback;
- no infinite retries.

---

## Phase 7 Retest Gate

Route switching policy is acceptable when:

- [ ] previous audio is checkpointed before route attempt;
- [ ] Bluetooth is attempted at most once automatically per route event;
- [ ] failed Bluetooth falls back to built-in mic;
- [ ] failed routes are quarantined temporarily;
- [ ] waiting state is stable and explainable;
- [ ] Finish is always available;
- [ ] user pause prevents auto-resume;
- [ ] first buffer validation is required before Recording UI;
- [ ] no infinite automatic retry loop exists.

---

## Summary

The app should not chase every route change endlessly.

The correct MVP policy is:

```text
try safely
validate honestly
fallback conservatively
wait visibly
finish reliably
```

This makes route switching understandable and protects the core promise of wawa-note: the user’s captured information is safe.
