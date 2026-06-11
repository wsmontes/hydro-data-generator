# Recording Reliability — Phase 5 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Pipeline and transcription gating
Last updated: 2026-06-10

This document captures Phase 5 of the recording reliability audit: ensuring the pipeline never produces an empty, stale, or partial transcript when valid user audio exists.

The key product rule:

> If valid audio bytes exist, the pipeline must either process them correctly or block with a recoverable artifact error. It must never silently produce an empty transcript.

---

## Phase 5 Goal

Build a reliable gate between recording finalization and content processing.

After `Finish`, the app must validate recording artifacts before transcription or analysis begins.

Pipeline execution must be conditional on artifact integrity, not merely on item status or existence of `audio.m4a`.

---

## Current Failure Class

Observed real-device issue:

```text
User speaks before Bluetooth route change.
Route change enters switching/waiting.
Finish completes.
Transcript misses the beginning or is almost empty.
Only a later phrase is captured.
```

This can happen if:

- `segment-000` exists but is ignored;
- `audio.m4a` was created from only a later segment;
- concat failed silently;
- resolver preferred stale `audio.m4a`;
- pipeline processed the wrong file;
- transcription failed but item was marked as processed or empty.

---

## Required Processing Contract

Pipeline must not decide input by guessing.

It must receive or produce a structured `AudioProcessingInput`.

Suggested type:

```swift
struct AudioProcessingInput: Sendable {
    enum Source: Sendable {
        case validatedSingleFile(URL)
        case validatedSegments([URL])
        case unavailable
        case invalid(AudioArtifactValidationResult)
    }

    let itemId: UUID
    let source: Source
    let validation: AudioArtifactValidationResult
    let concatResult: ConcatenationResult?
}
```

Then pipeline behavior becomes explicit:

```text
validatedSingleFile -> transcribe file
validatedSegments -> transcribe segments in manifest order
invalid -> do not transcribe; mark retryable artifact issue
unavailable -> no audio processing
```

---

## Required Gate After Finish

`RecordingCoordinator.stopRecording()` should not directly call `pipeline.process(...)` just because stop completed.

It should perform this sequence:

```text
1. stop physical capture
2. finalize writer
3. finalize manifest
4. persist manifest
5. run artifact validation
6. run concat only if validation allows
7. inspect ConcatenationResult
8. select AudioProcessingInput
9. trigger pipeline only with validated input
```

Recommended pseudo-flow:

```swift
let validation = AudioArtifactValidator.validate(itemId: itemId)

if validation.canUseAudioM4A {
    pipeline.process(itemId, input: .validatedSingleFile(audioURL), using: modelContext)
    return
}

if validation.canUseSegments {
    let concat = await AudioSegmentConcatenator.concatenate(manifest: m, meetingId: itemId)

    switch concat.status {
    case .completed:
        pipeline.process(itemId, input: .validatedSingleFile(concat.outputURL!), using: modelContext)
    case .partial:
        // Either process segments directly with warning, or block as retryable.
        pipeline.process(itemId, input: .validatedSegments(validation.validSegmentURLs), using: modelContext)
    case .failed:
        markArtifactIssue(itemId, validation: validation, concat: concat)
    }
    return
}

markArtifactIssue(itemId, validation: validation, concat: nil)
```

---

## Transcription Requirements

### Requirement 1 — Transcription must support segmented fallback

If concat fails but valid ordered segments exist, transcription should support:

```swift
transcribeSegments(_ urls: [URL]) async throws -> Transcript
```

The resulting transcript should preserve segment order from the manifest.

If timestamps are available, offset each segment's transcript by accumulated segment duration.

If timestamps are not available, preserve text order and mark segment boundaries internally.

---

### Requirement 2 — Empty transcript is not success when audio exists

If transcription returns no text but validation says audio bytes exist, this must not be treated as normal success.

Classify as:

```text
transcription produced empty result despite valid audio
```

This should become a retryable/diagnostic state, not final success.

---

### Requirement 3 — Pipeline must distinguish no audio vs failed audio processing

These are different states:

```text
No audio available
Audio exists but artifacts are inconsistent
Audio exists but transcription failed
Audio exists but transcription returned empty
Audio processed successfully
```

The UI and item status should not collapse these into the same failure.

---

### Requirement 4 — Pipeline should not overwrite good transcript with bad transcript

If an item already has a valid transcript and a later automatic retry produces an empty or failed transcript, do not replace the valid one.

Use a staging file first:

```text
transcript.pending.json
```

Then commit only after validation:

```text
pending transcript has text or explicit valid empty-audio reason
```

---

## Recommended Item Processing States

The current item status may be too coarse for artifact reliability.

Recommended additional states or metadata:

```swift
enum ProcessingArtifactState: Codable {
    case none
    case audioValidated
    case audioArtifactIssue
    case transcriptionInProgress
    case transcriptionSucceeded
    case transcriptionEmptyDespiteAudio
    case transcriptionFailedRetryable
    case analysisInProgress
    case analysisSucceeded
    case analysisFailedRetryable
}
```

If adding enum fields is too large for MVP, store a diagnostic artifact file:

```text
processing.report.json
```

with:

```swift
struct ProcessingReport: Codable {
    let itemId: UUID
    let createdAt: Date
    let validation: AudioArtifactValidationResult
    let concatResult: ConcatenationResult?
    let selectedInput: String
    let transcriptionStatus: String
    let transcriptionTextLength: Int
    let errorDescription: String?
}
```

---

## Artifact Issue Handling

When artifact validation fails but valid segment bytes exist, the item should remain recoverable.

Recommended behavior:

- preserve all segment files;
- preserve manifest;
- do not delete `audio.m4a` unless replacing safely;
- mark processing as retryable;
- show user-friendly status such as:

```text
Audio saved. Transcript needs retry.
```

not:

```text
Recording failed.
```

---

## Pipeline Logging Requirements

Every pipeline run for audio should log:

```text
itemId
hasManifest
manifestSegmentCount
validSegmentCount
totalValidSegmentBytes
audioM4AExists
audioM4ASize
concatStatus
selectedProcessingInput
transcriptionStarted
transcriptionInputCount
transcriptionResultTextLength
transcriptionCommitted
analysisStarted
analysisCommitted
errorDescription
```

This is required until reliability stabilizes.

---

## Required File-Level Changes

Likely files involved:

- `RecordingCoordinator.swift`
- `ContentPipelineService.swift`
- `ContentExtractionService.swift`
- transcription provider/service files
- `FileArtifactStore.swift`
- `AudioSegmentConcatenator.swift`
- `AudioAssetResolver.swift`

The exact implementation should reuse the shared artifact validator from Phase 1/3/4.

---

## Acceptance Tests

### Test 1 — Normal recording

Setup:

```text
one valid segment
concat completed
```

Expected:

- pipeline starts;
- transcript generated;
- analysis generated;
- no artifact warnings.

---

### Test 2 — Valid segment but no audio.m4a

Setup:

```text
manifest exists
segment-000 exists size > 0
audio.m4a missing
```

Expected:

- pipeline does not fail just because `audio.m4a` is missing;
- concat or segment fallback runs;
- transcript includes segment content.

---

### Test 3 — Missing first segment

Setup:

```text
manifest expects segment-000 and segment-001
segment-000 missing
segment-001 exists
```

Expected:

- pipeline does not transcribe segment-001 as if complete;
- artifact issue is recorded;
- valid files are preserved.

---

### Test 4 — Empty transcription despite valid audio

Setup:

```text
valid audio bytes exist
transcription provider returns empty text
```

Expected:

- pipeline marks `transcriptionEmptyDespiteAudio` or equivalent;
- does not overwrite a previous valid transcript;
- item remains retryable.

---

### Test 5 — Failed Bluetooth route with preserved pre-switch audio

Setup:

```text
start on iPhone
speak part one
route switch fails
Finish
segment-000 valid
```

Expected:

- transcript includes part one; OR
- pipeline is blocked with explicit artifact issue;
- no silent empty/partial transcript.

---

## Phase 5 Retest Gate

Do not tune UI polish or Bluetooth behavior until:

- [ ] pipeline receives validated audio input;
- [ ] concat result is inspected;
- [ ] missing segments block complete processing;
- [ ] segment fallback exists or artifact issue is explicit;
- [ ] empty transcript despite audio is treated as retryable failure;
- [ ] good transcript cannot be overwritten by empty retry;
- [ ] processing report is stored or logged.

---

## Summary

The pipeline is the last place where data loss can become invisible.

Even if recording and files are correct, the user experiences data loss if the app writes an empty transcript or partial transcript without warning.

The pipeline must become conservative:

> Do not process unless input is validated. Do not mark success unless output is meaningful. Do not overwrite good data with bad data.
