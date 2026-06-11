# Recording Reliability — Phase 1 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Segment and manifest integrity
Last updated: 2026-06-10

This document captures the Phase 1 findings from the recording reliability audit. These findings should be addressed before continuing Bluetooth behavior optimization.

---

## Phase 1 Goal

Make the segment layer impossible to corrupt silently.

The app must guarantee that valid audio already captured by the user cannot be overwritten, removed from the manifest, ignored by concatenation, or processed as an empty transcript.

---

## Finding 1 — `startNextSegmentForExistingRecording` must set `_currentMeetingId`

### File

`wawa-note/Audio/AudioFileWriter.swift`

### Problem

`AudioFileWriter.startNextSegmentForExistingRecording(...)` opens the next segment for an existing logical recording, but the visible implementation does not clearly set:

```swift
_currentMeetingId = meetingId
```

before opening the new segment.

This is dangerous because `_closeCurrentSegment()` depends on `_currentMeetingId`:

```swift
guard _audioFile != nil, let meetingId = _currentMeetingId else { return nil }
```

If `_currentMeetingId` is `nil` or stale after a physical restart, the app can create a new segment file but later fail to close/finalize it into the manifest.

### Risk

- Segment file exists on disk but is not finalized.
- `ClosedSegmentInfo` becomes `nil`.
- Coordinator does not update manifest.
- Concat/pipeline sees inconsistent artifacts.
- User audio may be ignored even though it was recorded.

### Required fix

Inside every method that opens a segment for an existing recording:

```swift
_currentMeetingId = meetingId
try _openSegment(meetingId: meetingId, format: format)
```

### Acceptance

- Open `segment-001` for an existing recording.
- Close `segment-001`.
- `ClosedSegmentInfo` is non-nil.
- Coordinator receives correct index, filename, end date, and file size.

---

## Finding 2 — Writer-adjusted segment index must propagate to manifest

### File

`wawa-note/Audio/AudioFileWriter.swift`

### Problem

`_openSegment(...)` can now skip forward if a target segment file already exists with bytes. This is good because it prevents overwriting user audio.

But if the writer adjusts the index, the manifest must record the actual opened segment, not the originally requested index.

### Risk

Example:

```text
manifest asks for segment-001
writer sees segment-001 exists
writer skips to segment-002
manifest still records segment-001
```

Result:

- Manifest points to wrong file.
- Concat may skip actual audio.
- Pipeline may transcribe wrong segment.
- Debug reports become misleading.

### Required fix

After opening a new segment, build `RecordingSegment` using the writer's actual state:

```swift
let actualIndex = fileWriter.segmentIndex
let actualFileName = fileWriter.currentFileURL?.lastPathComponent
```

Do not assume `manifestNextIndex` is the final opened index.

### Acceptance

- If `segment-001` exists, writer skips to `segment-002`.
- Manifest records `segment-002`, not `segment-001`.
- Disk and manifest agree.

---

## Finding 3 — Manifest updates must target segment by `index` or `fileName`

### File

`wawa-note/Connectivity/RecordingCoordinator.swift`

### Problem

Current segment callbacks still appear to finalize the last segment in the manifest array.

This is fragile in an async route-change system. The last segment in the array is not always guaranteed to be the segment described by `ClosedSegmentInfo`.

### Risk

- A close callback for one segment updates another segment.
- `fileSize` and `endedAt` are assigned to the wrong manifest entry.
- Concat receives misleading metadata.
- A valid early segment may look unfinished or invalid.

### Required fix

Use `closedInfo.index` or `closedInfo.fileName` as the matching key:

```swift
if let idx = m.segments.firstIndex(where: {
    $0.index == closedInfo.index || $0.fileName == closedInfo.fileName
}) {
    m.segments[idx].endedAt = closedInfo.endedAt
    m.segments[idx].fileSize = closedInfo.fileSize
} else {
    AppLog.error("audio", "Closed segment not found in manifest: index=\(closedInfo.index) file=\(closedInfo.fileName)")
}
```

Apply this to:

- `onSegmentClosed`
- `onSegmentCreated`

### Acceptance

- Closing an older segment cannot accidentally update the newest entry.
- Route change callbacks cannot corrupt another segment's metadata.
- Logs clearly report if a closed segment is missing from the manifest.

---

## Finding 4 — Concatenator can hide missing segments

### File

`wawa-note/Audio/AudioSegmentConcatenator.swift`

### Problem

The concatenator builds input URLs by keeping only segment files that exist.

This means a segment can be present in the manifest but silently dropped if the file is missing.

If only one file remains, the current behavior may copy that one file to `audio.m4a` and allow the pipeline to process a partial recording.

### Risk

This matches the observed failure:

```text
segment-000 missing or ignored
segment-001 exists
concat creates audio.m4a from segment-001 only
pipeline transcribes only a later phrase
user loses the beginning
```

### Required fix

Change concatenation from a silent `Void` operation to a validated operation returning a structured result:

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

### Acceptance

- Missing manifest segment is reported.
- Zero-byte segment is reported.
- Partial concat cannot masquerade as complete recording.
- Pipeline does not process partial output as if it represented the whole manifest.

---

## Finding 5 — `AudioAssetResolver` should not prefer stale `audio.m4a` over manifest

### File

`wawa-note/Audio/AudioAssetResolver.swift`

### Problem

The resolver checks `audio.m4a` first and returns `.singleFileReady` if it exists, before validating `recording.manifest.json`.

For segmented recordings, this is unsafe because `audio.m4a` is a derived artifact, not the source of truth.

### Risk

- Stale `audio.m4a` hides valid segments.
- Partial `audio.m4a` hides missing segments.
- Playback/export/transcription may use the wrong audio.

### Required fix

If a manifest exists:

1. Validate manifest first.
2. Validate segment files.
3. Validate whether `audio.m4a` corresponds to the manifest.
4. Regenerate or ignore `audio.m4a` if stale or partial.

Only use `audio.m4a` directly when no manifest exists.

### Acceptance

- Segmented recording uses manifest as source of truth.
- Stale `audio.m4a` does not hide valid segments.
- Derived audio is regenerated if it does not match manifest.

---

## Finding 6 — Phase 1 must block pipeline on artifact inconsistency

### Files

- `RecordingCoordinator.swift`
- `AudioSegmentConcatenator.swift`
- pipeline/transcription services

### Problem

The pipeline must not produce an empty or partial transcript when valid audio exists or when artifact inconsistency is detected.

### Required fix

Before `pipeline.process(...)`, run manifest/disk validation.

If validation fails:

- do not generate empty transcript;
- do not process partial `audio.m4a` as complete;
- preserve all valid segment files;
- show/log an artifact validation error;
- keep item recoverable/retryable.

### Acceptance

Manual test:

```text
Start recording on iPhone.
Speak "part one".
Trigger route switch / Bluetooth confusion.
Finish.
```

Expected:

- `segment-000` exists.
- `segment-000` has non-zero size.
- Manifest references `segment-000`.
- No later segment overwrites `segment-000`.
- Transcript includes part one, OR pipeline is blocked with a clear artifact validation error instead of silently producing an empty or partial transcript.

---

## Recommended Claude Code Task

Focus only on Phase 1.

Do not continue Bluetooth auto-resume behavior until segment and manifest integrity are mechanically safe.

Priority order:

1. Fix `_currentMeetingId` in additional-segment opening paths.
2. Ensure actual writer segment index/filename propagate to manifest.
3. Update manifest entries by `closedInfo.index` or `closedInfo.fileName`.
4. Add manifest/disk validation helper.
5. Make concatenation structured and non-silent.
6. Block pipeline when artifacts are inconsistent.

---

## Retest Gate

Do not run the next Bluetooth behavior test until these are true:

- [ ] Non-empty existing segments cannot be overwritten.
- [ ] Additional segment open path preserves `_currentMeetingId`.
- [ ] Closed segment metadata updates the matching manifest entry.
- [ ] Manifest/disk validation exists.
- [ ] Concat reports missing/partial segments.
- [ ] Pipeline does not process partial/stale audio as complete.
