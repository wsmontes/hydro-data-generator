# Recording Reliability Audit Tracker

Status: active
Owner: wawa-note recording reliability workstream
Last updated: 2026-06-10

This document tracks the stabilization work for recording reliability, microphone route changes, segmented audio, artifact validation, transcription readiness, and active recording UX.

The goal is not to make Bluetooth perfect first. The goal is to make recording trustworthy:

> Valid audio already captured by the user must never be lost, overwritten, ignored, or processed as empty.

---

## 1. Product Rule

The app has two different recording layers.

### Logical recording session

User-facing recording.

- One `KnowledgeItem`.
- One recording screen session.
- One `RecordingManifest`.
- Continues until the user taps `Finish`.
- Must survive route changes, microphone loss, Bluetooth failure, app interruptions, and partial processing failure.

### Physical capture session

Audio-hardware-facing capture.

- Disposable.
- Bound to one concrete audio route/input.
- Owns one `AVAudioSession`/`AVAudioEngine`/tap/open segment lifecycle.
- Can stop and restart many times inside one logical recording.

Route changes must restart the physical capture, not restart the logical recording.

---

## 2. Non-Negotiable Invariants

### I-001 — Existing user audio is immutable

Once a segment file has non-zero bytes and is referenced by the manifest, it belongs to the user.

It must never be:

- overwritten,
- reopened,
- deleted by route recovery,
- removed from the manifest,
- silently ignored by concatenation,
- silently ignored by transcription.

### I-002 — `recording.manifest.json` is the source of truth

For segmented recordings, the manifest is authoritative.

`audio.m4a` is a derived compatibility artifact.

If a manifest exists, playback, export, concat, and transcription must validate against it.

### I-003 — `AudioFileWriter.startRecording(...)` is only for a new logical recording

This method resets segment index to zero. It must never be called during route switching or physical capture restart inside an existing recording.

Route switching must use an additional-segment path such as:

```swift
startNextSegmentForExistingRecording(...)
```

### I-004 — Segment index must be monotonic

A new physical segment must use the next available index from the manifest:

```swift
let nextIndex = (manifest.segments.map(\.index).max() ?? -1) + 1
```

The writer may scan forward to avoid overwriting existing files, but it must never move backward.

### I-005 — Finish always wins

`Finish` must work from every active logical recording state:

- recording,
- paused,
- switching microphone,
- validating route,
- waiting for input,
- interrupted,
- failed but with valid committed audio.

`Finish` must cancel route recovery, validation, probe tasks, timers, and audio engine work before finalizing committed segments.

### I-006 — Pipeline must not process empty audio if valid segments exist

Before transcription/analysis, the app must prove that the pipeline input is valid.

Valid input means either:

- `audio.m4a` exists, is non-empty, and corresponds to the manifest, or
- one or more manifest segments exist and have non-zero bytes.

If concat fails but valid segments exist, transcription must fall back to segmented audio instead of producing an empty transcript.

### I-007 — UI must not claim recording unless physical capture is validated

The UI may show `Recording` only after:

- engine is running,
- tap is installed,
- first valid audio buffer was received after route start/restart,
- current segment is committed,
- useful timer is advancing.

---

## 3. Current Known Failure Modes

### F-001 — Bluetooth route change enters waiting and never recovers

Observed behavior:

- Start on iPhone mic.
- Insert Jabra Bluetooth earbud.
- UI shows `Switching microphone`.
- App enters `Microphone disconnected / waiting for input`.
- Source may still show iPhone.
- Timer freezes.
- App does not resume.

Status: active investigation.

Primary suspicion:

- `waitingForUsableInput` became a passive or semi-passive state.
- The app updates route/source label without starting a full physical capture restart.

Required result:

- If iPhone mic is available, start a new physical capture from zero.
- If Bluetooth is available and validates, start Bluetooth segment.
- If neither validates, wait with `Finish` available.

---

### F-002 — Pre-switch audio lost after route change

Observed behavior:

- Start on iPhone.
- Speak before Bluetooth route change.
- Route change fails or partially restarts.
- Final transcript contains only a later phrase.
- Early audio is missing.

Status: critical.

Primary suspicions:

- `segment-000` overwritten by a mistaken logical restart.
- Manifest points only to later segment.
- Concatenator ignores missing/zero-byte segment and produces `audio.m4a` from remaining segment.
- Pipeline transcribes stale or partial `audio.m4a`.

Required result:

- `segment-000` must remain on disk.
- `segment-000` must remain in manifest.
- Concat must include it or fail loudly.
- Pipeline must not process a partial derived file as if it represented the whole recording.

---

### F-003 — Crash at `Switching microphone`

Observed behavior:

- Start recording on iPhone.
- Insert Bluetooth earbud.
- UI reaches `Switching microphone`.
- App crashes.

Status: active investigation.

Primary suspicions:

- route change, waiting probe, and user actions can overlap;
- `removeTap` / `installTap` / engine reset are not fully serialized;
- input node accessed during unstable route state;
- engine teardown and restart are mixed on same instance;
- route settle delay insufficient.

Required result:

- No crash during route change.
- Previous segment checkpointed before risky audio operations.
- If restart cannot proceed, app enters waiting with `Finish` available.

---

### F-004 — Transcript empty despite valid audio

Observed behavior:

- Finish completes.
- Transcript is empty or missing.
- Valid segment may exist on disk.

Status: active investigation.

Primary suspicions:

- Pipeline runs before concat is ready.
- Resolver chooses stale `audio.m4a` before checking manifest.
- Transcription service does not fallback to segments.
- Audio artifact validation is not gating pipeline.

Required result:

- No processing as empty while valid segments exist.
- Debug artifact report logged on every Finish.

---

## 4. Stabilization Phases

## Phase 0 — Freeze risky Bluetooth behavior

Goal: stop data loss while investigating.

Rules:

- Do not optimize Bluetooth auto-resume yet.
- Do not introduce new route switching UX experiments.
- Prioritize segment preservation and artifact validation.

Acceptance:

- If route switch fails, existing audio is preserved.
- Finish works.
- Transcript includes valid pre-switch audio.

Status: in progress.

---

## Phase 1 — Segment and manifest integrity

Goal: make the segment layer impossible to corrupt silently.

Tasks:

- [ ] Ensure `startRecording(format:meetingId:)` is used only once per logical recording.
- [ ] Ensure physical route restart uses only additional-segment APIs.
- [ ] Ensure any method opening a segment for an existing recording sets `_currentMeetingId = meetingId`.
- [ ] Ensure no non-empty segment file can be overwritten.
- [ ] Ensure manifest is updated by matching `closedInfo.index` or `closedInfo.fileName`, not by assuming the last segment.
- [ ] Ensure every segment open/close logs index, filename, byte size, and manifest state.
- [ ] Add validation that manifest indices and disk files agree.

Acceptance tests:

- [ ] Start recording, speak part one, force route confusion, Finish.
- [ ] `segment-000` exists and has bytes.
- [ ] Manifest references `segment-000`.
- [ ] Transcript includes part one.
- [ ] No later segment overwrites `segment-000`.

Status: in progress.

---

## Phase 2 — Crash-safe physical capture restart

Goal: route change cannot crash the app.

Tasks:

- [ ] Serialize all physical capture operations through one gate.
- [ ] Prevent route recovery, waiting probe, user pause, and Finish from overlapping.
- [ ] Make `removeTap` idempotent using `isTapInstalled`.
- [ ] Make `installTap` idempotent and safe.
- [ ] Separate old engine teardown from new engine startup.
- [ ] Deactivate/reconfigure session safely.
- [ ] Wait for route settle before reading input format or installing tap.
- [ ] Validate sample rate and channel count before opening a new segment.
- [ ] Abort to waiting state instead of crashing when input is invalid.

Acceptance tests:

- [ ] Start on iPhone, insert Bluetooth, no crash.
- [ ] Previous segment remains valid.
- [ ] Finish works after failed route change.

Status: in progress.

---

## Phase 3 — Concatenation as a validated operation

Goal: `audio.m4a` must represent the manifest or fail loudly.

Tasks:

- [ ] Change `AudioSegmentConcatenator.concatenate` from `Void` to structured result.
- [ ] Report included segments, missing segments, zero-byte segments, skipped tracks, export status, and output size.
- [ ] Do not silently ignore missing manifest segments.
- [ ] If manifest contains N valid segments, concat result must include all N or report failure.
- [ ] If only one valid segment exists, copy it only after validating it is the expected segment.
- [ ] Do not create `audio.m4a` from only later segments while earlier manifest segments are missing.

Suggested result type:

```swift
struct ConcatenationResult {
    enum Status {
        case completed
        case partial
        case failed
    }

    let status: Status
    let outputURL: URL?
    let outputSize: Int64
    let includedSegments: [RecordingSegment]
    let missingSegments: [RecordingSegment]
    let zeroByteSegments: [RecordingSegment]
    let skippedSegments: [RecordingSegment]
    let errorDescription: String?
}
```

Acceptance:

- [ ] Missing `segment-000` blocks normal pipeline.
- [ ] Partial concat never masquerades as complete recording.
- [ ] Logs clearly show concat input and output.

Status: pending.

---

## Phase 4 — Audio asset resolver correctness

Goal: resolver must not prefer stale derived audio over valid manifest segments.

Tasks:

- [ ] If manifest exists, validate manifest first.
- [ ] Use `audio.m4a` only if it is known to correspond to the manifest.
- [ ] Regenerate `audio.m4a` if stale or incomplete.
- [ ] Remove or replace public `fatalError` property in `AudioAssetResolver`.
- [ ] Expose `AudioAssetState` with enough detail for UI and pipeline.

Acceptance:

- [ ] Segmented recording playback/export works when `audio.m4a` is missing.
- [ ] Stale `audio.m4a` does not hide valid segments.
- [ ] No public resolver API can crash the app by design.

Status: pending.

---

## Phase 5 — Pipeline and transcription gating

Goal: transcript must never be empty if valid audio exists.

Tasks:

- [ ] Gate `pipeline.process` behind artifact validation.
- [ ] If concat succeeds completely, process `audio.m4a`.
- [ ] If concat fails but valid segments exist, process segments directly or mark as retryable without losing audio.
- [ ] If no valid audio exists, mark recording artifact failure clearly.
- [ ] Do not show generic recording error when valid audio was preserved.
- [ ] Store artifact validation report for debugging.

Acceptance:

- [ ] No-route-change recording transcribes.
- [ ] Failed Bluetooth switch still transcribes pre-switch segment.
- [ ] Partial concat does not produce empty transcript.

Status: pending.

---

## Phase 6 — Active recording UI state model

Goal: UI must communicate what is really happening.

States must distinguish:

- logical recording active,
- physical capture recording,
- switching microphone,
- validating microphone,
- waiting for input,
- paused by user,
- stopping/finalizing,
- failed but audio saved,
- failed with no audio.

Tasks:

- [ ] Show `Recording` only when physical capture is validated.
- [ ] Show `Switching microphone...` during teardown/restart.
- [ ] Show which input is being tried: iPhone, Jabra, Bluetooth, etc.
- [ ] Show attempt status or stable waiting status.
- [ ] Keep `Finish` visible and enabled in active logical states.
- [ ] Do not show `Pause` when capture is already unavailable.
- [ ] Do not show `Recording Error` if audio exists and was saved.

Acceptance:

- [ ] User can tell whether the app is recording, switching, waiting, or finalizing.
- [ ] User never gets stuck without Finish.

Status: pending.

---

## Phase 7 — Route switching product policy

Goal: decide conservative behavior before optimizing Bluetooth.

Recommended policy for MVP:

1. On route change, checkpoint current segment first.
2. Stop and release current physical capture.
3. Let iOS settle.
4. Try the active/default usable route once.
5. If Bluetooth HFP does not deliver a buffer, fallback to built-in mic.
6. If built-in mic validates, continue recording on iPhone.
7. If no mic validates, wait with Finish available.
8. Do not retry Bluetooth forever.
9. Offer manual `Use iPhone Microphone` later if needed.

Status: pending.

---

## 5. Current Code Areas Under Audit

### Audio capture

Files:

- `wawa-note/Audio/AudioCaptureService.swift`
- `wawa-note/Audio/AudioSessionManager.swift`
- `wawa-note/Audio/AudioFileWriter.swift`

Audit focus:

- route change notification handling,
- physical capture restart,
- engine/tap lifecycle,
- writer lifecycle,
- segment open/close correctness,
- crash-safety.

### Recording coordination

Files:

- `wawa-note/Connectivity/RecordingCoordinator.swift`
- `wawa-note/UI/Home/HomeView.swift`
- `CaptureViewModel` location TBD if separate.

Audit focus:

- logical vs physical state,
- manifest ownership,
- UI state mirroring,
- Finish behavior,
- pipeline trigger timing.

### Artifacts and audio resolution

Files:

- `wawa-note/Storage/FileArtifactStore.swift`
- `wawa-note/Audio/AudioSegmentConcatenator.swift`
- `wawa-note/Audio/AudioAssetResolver.swift`

Audit focus:

- manifest validation,
- segment file validation,
- audio.m4a derivation,
- stale audio detection,
- playback/export readiness.

### Processing pipeline

Files to locate / audit:

- `ContentPipelineService.swift`
- `ContentExtractionService.swift`
- transcription engine files,
- processing queue files.

Audit focus:

- audio input selection,
- segmented transcription fallback,
- pipeline gating,
- failure handling.

---

## 6. Manual Test Log

Use this section to append real-device test results.

### Template

```text
Date/time:
Build/commit:
Device:
Input before test:
Action:
Observed UI:
Timer behavior:
Audio level behavior:
Buttons visible:
Finish result:
Transcript result:
Artifact report:
Conclusion:
Next action:
```

---

### Test — iPhone to Jabra, waiting, transcript lost beginning

Date/time: 2026-06-10
Build/commit: around `bec6460` / latest main during audit
Device: iPhone 14 Plus target device
Input before test: iPhone microphone
Action: start recording, speak, insert Jabra Bluetooth earbud
Observed UI: iPhone shown, then switching microphone, then iPhone microphone not delivering audio
Timer behavior: stopped during switching/waiting
Buttons visible: Finish available
Finish result: recording error shown
Transcript result: beginning missing; only later phrase captured
Conclusion: critical regression; likely segment manifest/concat/pipeline integrity issue, not only route switching
Next action: Phase 1 and Phase 3 before more Bluetooth optimization

---

## 7. Definition of Done for Recording Reliability MVP

Recording reliability is acceptable only when all of these pass:

- [ ] Normal iPhone recording transcribes correctly.
- [ ] Finish from normal recording works.
- [ ] Finish from switching/waiting works.
- [ ] App does not crash when Bluetooth appears.
- [ ] App does not crash when Bluetooth disappears.
- [ ] Pre-switch audio is preserved when route switch fails.
- [ ] Pre-switch audio is transcribed when route switch fails.
- [ ] No non-empty segment file is overwritten.
- [ ] `audio.m4a` is never treated as authoritative when manifest says otherwise.
- [ ] Pipeline never produces empty transcript while valid audio bytes exist.
- [ ] UI never shows fake Recording when capture is unavailable.
- [ ] User can always tell what the app is doing.

---

## 8. Next Immediate Work Item

Before another Bluetooth behavior experiment, complete Phase 1:

> Make segment and manifest integrity mechanically safe.

Then complete the minimum of Phase 3:

> Make concatenation return structured validation instead of silently ignoring missing segments.

Only after those pass should route-switch behavior be tested again.
