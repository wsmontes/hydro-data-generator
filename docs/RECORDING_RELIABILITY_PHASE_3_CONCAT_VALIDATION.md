# Recording Reliability — Phase 3 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Concatenation as a validated operation
Last updated: 2026-06-10

This document captures Phase 3 of the recording reliability audit: making segment concatenation a validated, inspectable operation instead of a best-effort side effect.

The key product rule:

> `audio.m4a` is a derived artifact. `recording.manifest.json` is the source of truth for segmented recordings.

---

## Phase 3 Goal

Ensure that `audio.m4a` represents the manifest completely, or fails loudly with a structured result.

The app must never silently generate a partial `audio.m4a` from only the segments that happened to exist on disk.

---

## Current Code Risk

### File

`wawa-note/Audio/AudioSegmentConcatenator.swift`

### Current behavior

The concatenator sorts manifest segments and then builds URLs using only files that exist:

```swift
let urls: [URL] = sortedSegments.compactMap { seg in
    let url = store.segmentURL(for: meetingId, fileName: seg.fileName)
    return FileManager.default.fileExists(atPath: url.path) ? url : nil
}
```

Then:

- if one URL remains, it copies that one file to `audio.m4a`;
- if multiple URLs remain, it creates an `AVMutableComposition`;
- missing segments are not reported;
- zero-byte segments are not distinguished;
- invalid audio tracks are skipped;
- export failures are not returned to caller;
- function returns `Void`.

### Why this is dangerous

If the manifest contains:

```text
segment-000
segment-001
```

but only `segment-001` exists, the current implementation can create `audio.m4a` from `segment-001` only.

The pipeline may then transcribe only a later phrase and appear to have lost the beginning of the recording.

This matches the observed real-device failure.

---

## Required Design

Replace best-effort concatenation with a validated operation.

### New result type

```swift
struct ConcatenationResult: Sendable {
    enum Status: Sendable {
        case completed
        case partial
        case failed
    }

    let status: Status
    let outputURL: URL?
    let outputSize: Int64
    let expectedSegments: [RecordingSegment]
    let includedSegments: [RecordingSegment]
    let missingSegments: [RecordingSegment]
    let zeroByteSegments: [RecordingSegment]
    let unreadableSegments: [RecordingSegment]
    let skippedSegments: [RecordingSegment]
    let errorDescription: String?
}
```

The public API should become:

```swift
static func concatenate(
    manifest: RecordingManifest,
    meetingId: UUID
) async -> ConcatenationResult
```

---

## Validation Rules

### Rule 1 — Manifest segments are expected inputs

Every segment in the manifest is expected.

Do not use `compactMap` to silently drop missing files.

Instead, classify every manifest segment:

- exists and size > 0;
- missing;
- exists but size == 0;
- exists but cannot load audio track;
- included successfully.

---

### Rule 2 — Missing manifest segments must produce partial or failed status

If any manifest segment is missing, the result cannot be `.completed`.

Possible outcomes:

```text
missing segment + no valid segments => failed
missing segment + some valid segments => partial
```

The caller decides whether partial output may be used for playback/export, but the pipeline must not treat it as a complete recording.

---

### Rule 3 — Zero-byte segments must be reported

A zero-byte file is not valid recording audio.

It must be recorded in `zeroByteSegments` and excluded from normal concat.

---

### Rule 4 — Unreadable audio tracks must be reported

If `AVAsset(url:)` cannot produce an audio track, the segment must be reported as unreadable/skipped.

Do not just `continue` silently.

---

### Rule 5 — Single valid segment copy must also be validated

If there is only one valid segment, copying it to `audio.m4a` is acceptable only when:

- no manifest segment is missing;
- no expected non-zero segment was skipped;
- the copied output exists;
- output size > 0.

If `segment-000` is missing and only `segment-001` remains, do not produce a `.completed` result.

---

### Rule 6 — Export status must be inspected

If `AVAssetExportSession` fails, result must be `.failed` or `.partial`, with error detail.

Do not return success only because the function completed.

Capture:

- `export.status`
- `export.error?.localizedDescription`
- output file existence
- output file size

---

### Rule 7 — Remove stale `audio.m4a` only at the correct time

Do not remove the previous `audio.m4a` until validation has identified that a new complete output can be written.

Safer approach:

1. write to temporary output, e.g. `audio.pending.m4a`;
2. validate export result and file size;
3. atomically replace `audio.m4a`;
4. if failed, leave old file alone or mark it stale.

This prevents failed concat from deleting a previously valid playback file.

---

## Caller Behavior

### RecordingCoordinator

After `Finish`, coordinator should:

1. finalize manifest;
2. validate manifest/disk artifacts;
3. call `AudioSegmentConcatenator.concatenate(...)`;
4. inspect `ConcatenationResult`;
5. only run pipeline if result is safe.

Suggested behavior:

```swift
let result = await AudioSegmentConcatenator.concatenate(manifest: m, meetingId: itemId)

switch result.status {
case .completed:
    pipeline.process(itemId, using: modelContext)
case .partial:
    // Do not pretend this is complete.
    // Either process segmented valid audio directly with warning,
    // or mark item as needs audio recovery.
case .failed:
    // Preserve segments; mark retryable artifact failure.
}
```

### Content pipeline

Pipeline must not assume `audio.m4a` is authoritative if manifest exists.

If concat is partial or failed but valid segment files exist, the pipeline should either:

- transcribe segments directly, preserving order from the manifest; or
- block transcription with a clear artifact validation error.

It must never silently produce an empty transcript while valid segment bytes exist.

### AudioAssetResolver

If manifest exists, resolver should validate manifest first.

`audio.m4a` should be returned as ready only if it corresponds to the manifest.

---

## Recommended Implementation Steps

1. Add `ConcatenationResult` type.
2. Change `concatenate(...)` to return `ConcatenationResult`.
3. Replace `compactMap` with explicit segment classification.
4. Add file size checks for every segment.
5. Add readable audio-track validation.
6. Write export to temporary file first.
7. Replace final `audio.m4a` only after success.
8. Update `RecordingCoordinator` to inspect result before pipeline.
9. Update `AudioAssetResolver` to respect manifest-first semantics.
10. Add debug logs summarizing expected/included/missing/zero-byte/skipped segments.

---

## Required Logs

Every concat attempt should log:

```text
meetingId
manifest segment count
expected segment filenames
included segment filenames
missing segment filenames
zero-byte segment filenames
unreadable segment filenames
skipped segment filenames
output URL
output size
result status
error description
```

This is required until recording reliability is stable.

---

## Acceptance Tests

### Test 1 — Single normal segment

Setup:

```text
manifest: segment-000 exists, size > 0
```

Expected:

- result `.completed`
- `audio.m4a` exists
- output size > 0
- includedSegments contains `segment-000`
- no missing/zero-byte/skipped segments

---

### Test 2 — Two normal segments

Setup:

```text
manifest: segment-000, segment-001
both exist, both size > 0
```

Expected:

- result `.completed`
- output includes both in index order
- output size > 0
- pipeline may proceed

---

### Test 3 — Missing first segment

Setup:

```text
manifest: segment-000, segment-001
segment-000 missing
segment-001 exists
```

Expected:

- result `.partial` or `.failed`
- `segment-000` listed in missingSegments
- output must not be treated as complete
- pipeline must not process the remaining audio as full recording

---

### Test 4 — Zero-byte segment

Setup:

```text
manifest: segment-000 exists but size == 0
```

Expected:

- result `.failed`
- segment listed in zeroByteSegments
- no empty transcript generated

---

### Test 5 — Export failure

Setup:

```text
valid segment exists but export session fails
```

Expected:

- result `.failed`
- errorDescription populated
- old valid artifacts preserved
- no stale/partial `audio.m4a` treated as success

---

## Phase 3 Retest Gate

Do not continue to Bluetooth behavior tuning until:

- [ ] concat returns structured result;
- [ ] missing manifest segments are reported;
- [ ] zero-byte segments are reported;
- [ ] partial output cannot masquerade as complete;
- [ ] pipeline checks concat result;
- [ ] resolver uses manifest-first semantics;
- [ ] logs show exactly which files were included.

---

## Summary

The concatenator is currently too permissive. It can hide the exact class of data loss observed in real-device tests.

The fix is not to make concat more clever. The fix is to make it honest:

> either `audio.m4a` represents the manifest, or the app must say clearly that it does not.
