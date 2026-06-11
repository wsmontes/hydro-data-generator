# Recording Reliability — Phase 4 Findings

Status: active
Related tracker: `docs/RECORDING_RELIABILITY_AUDIT.md`
Phase: Audio asset resolver correctness
Last updated: 2026-06-10

This document captures Phase 4 of the recording reliability audit: making audio resolution manifest-first and preventing stale or partial `audio.m4a` from hiding the true segmented recording state.

The key product rule:

> For segmented recordings, `recording.manifest.json` is the source of truth. `audio.m4a` is only a derived compatibility artifact.

---

## Phase 4 Goal

Ensure that playback, export, and pipeline consumers never use a stale, partial, corrupt, or unrelated `audio.m4a` when a manifest exists.

If a manifest exists, the resolver must validate the manifest first.

---

## Current Code Risk

### File

`wawa-note/Audio/AudioAssetResolver.swift`

### Current behavior

The resolver checks for `audio.m4a` first:

```swift
let legacyURL = store.audioFileURL(for: itemId)
if FileManager.default.fileExists(atPath: legacyURL.path) {
    return .singleFileReady(legacyURL)
}
```

Only if `audio.m4a` is missing does it inspect `recording.manifest.json`.

### Why this is dangerous

For segmented recordings, this reverses the correct authority relationship.

It allows this failure:

```text
manifest: segment-000 + segment-001
audio.m4a: exists but was generated from segment-001 only
resolver: returns audio.m4a as ready
pipeline/playback/export: use partial audio
user: loses beginning of recording
```

This matches the real-device failure class observed during Bluetooth route switching.

---

## Required Design

The resolver must become manifest-first.

Suggested resolution order:

```text
if recording.manifest.json exists:
    read manifest
    validate manifest/disk files
    determine whether audio.m4a is current and complete
    if current and complete: return singleFileReady(audio.m4a, validatedAgainstManifest)
    if segments valid but audio.m4a missing/stale: return segmentsAvailable / needsRender
    if manifest invalid: return failed with structured validation report
else:
    fallback to legacy audio.m4a behavior
```

---

## Required State Model

The current `AudioAssetState` is too coarse.

Current states:

```swift
enum AudioAssetState {
    case unavailable
    case singleFileReady(URL)
    case segmentsAvailable(segmentCount: Int)
    case rendering
    case failed(String)
}
```

Recommended richer state:

```swift
enum AudioAssetState: Equatable, Sendable {
    case unavailable

    // Legacy audio only, no manifest.
    case legacySingleFileReady(URL, size: Int64)

    // Manifest exists and derived audio is confirmed to represent it.
    case renderedFromManifestReady(
        url: URL,
        size: Int64,
        segmentCount: Int,
        totalSegmentBytes: Int64
    )

    // Manifest exists, segments are valid, but derived audio needs rendering.
    case segmentsAvailable(
        segmentCount: Int,
        totalBytes: Int64,
        needsRender: Bool
    )

    // Manifest exists, but disk/manifest validation failed.
    case manifestInvalid(AudioArtifactValidationResult)

    case rendering
    case failed(String)
}
```

If this is too much for one patch, minimally distinguish:

```swift
case singleFileReady(URL, source: AudioAssetSource)
```

where:

```swift
enum AudioAssetSource {
    case legacy
    case renderedFromManifest
}
```

---

## Required Validation Input

The resolver should depend on a shared validation helper instead of duplicating checks.

Suggested helper:

```swift
struct AudioArtifactValidationResult: Sendable {
    let itemId: UUID
    let manifestExists: Bool
    let audioM4AExists: Bool
    let audioM4ASize: Int64
    let segmentCount: Int
    let validSegments: [RecordingSegment]
    let missingSegments: [RecordingSegment]
    let zeroByteSegments: [RecordingSegment]
    let orphanSegmentFiles: [String]
    let totalValidSegmentBytes: Int64
    let canUseSegments: Bool
    let canUseAudioM4A: Bool
    let preferredInput: PreferredInput

    enum PreferredInput: Sendable {
        case audioM4A
        case manifestSegments
        case none
    }
}
```

This helper should be used by:

- `AudioAssetResolver`
- `AudioSegmentConcatenator`
- `RecordingCoordinator` before pipeline
- export code
- playback code
- debug reports

---

## `audio.m4a` Freshness Rules

A derived `audio.m4a` can be considered fresh only if one of these is true:

### Option A — store render metadata

Create a small metadata file, for example:

```text
audio.render.json
```

With:

```swift
struct AudioRenderMetadata: Codable, Sendable {
    let manifestHash: String
    let segmentFingerprints: [SegmentFingerprint]
    let renderedAt: Date
    let outputFileName: String
    let outputSize: Int64
}

struct SegmentFingerprint: Codable, Sendable {
    let index: Int
    let fileName: String
    let fileSize: Int64
    let endedAt: Date?
}
```

Then `audio.m4a` is fresh only if metadata matches the current manifest and segment files.

### Option B — conservative MVP

If manifest exists and any segment exists, always treat `audio.m4a` as derived and potentially stale unless it was just produced by a successful `ConcatenationResult.completed` in the current flow.

For MVP, Option B is acceptable and safer.

---

## Resolver Behavior Requirements

### Requirement 1 — Manifest-first lookup

Do not return `audio.m4a` before checking the manifest.

```swift
if store.recordingManifestExists(for: itemId) {
    // manifest path first
} else {
    // legacy path
}
```

---

### Requirement 2 — Stale derived file must not hide segments

If manifest exists and valid segments exist, but `audio.m4a` is stale/unknown:

- return `.segmentsAvailable(...)` or equivalent;
- allow render-on-demand;
- do not return `.singleFileReady` blindly.

---

### Requirement 3 — Invalid manifest must be visible

If manifest exists but validation fails:

- return `.manifestInvalid(...)` or `.failed(...)` with structured reason;
- do not silently fall back to `audio.m4a` unless explicitly marked safe;
- do not hide missing `segment-000`.

---

### Requirement 4 — Remove public `fatalError`

The current resolver has a public property that calls `fatalError`:

```swift
var hasPlayableAudio: Bool {
    fatalError("Use state(for:) instead — this resolver is stateless per item.")
}
```

This should be removed or made unavailable in a non-crashing way.

Recommended:

```swift
@available(*, unavailable, message: "Use state(for:) instead")
var hasPlayableAudio: Bool { false }
```

or remove the property entirely if possible.

A public API should not crash the app by design.

---

### Requirement 5 — Rendering should return result, not fire-and-forget

Current resolver calls:

```swift
await AudioSegmentConcatenator.concatenate(...)
```

and then checks whether `audio.m4a` exists.

After Phase 3, it should inspect `ConcatenationResult`.

Recommended:

```swift
let result = await AudioSegmentConcatenator.concatenate(manifest: manifest, meetingId: itemId)

guard result.status == .completed,
      let url = result.outputURL,
      result.outputSize > 0 else {
    throw AudioAssetError.renderingFailed
}

return url
```

---

## Playback Behavior

Playback should use resolver output as follows:

```text
legacySingleFileReady -> play directly
renderedFromManifestReady -> play directly
segmentsAvailable -> render, validate, then play
manifestInvalid -> show recoverable audio error, do not pretend no audio exists
unavailable -> no audio UI
failed -> error UI
```

Important:

`manifestInvalid` is not the same as `unavailable`.

If valid segment files exist but derived audio is invalid, the UI should communicate:

```text
Audio exists, but needs repair/preparation.
```

not:

```text
No audio available.
```

---

## Export Behavior

Export should behave similarly:

```text
manifest valid + rendered audio fresh -> export audio.m4a
manifest valid + rendered audio missing/stale -> render then export
manifest partial/invalid -> block export or offer partial export clearly labeled
legacy audio only -> export audio.m4a
```

Do not export a partial derived file as if it were the full recording.

---

## Pipeline Behavior

The pipeline should not use `AudioAssetResolver` in a way that hides manifest issues.

If pipeline asks for an audio URL and manifest exists:

- resolver must return a complete validated URL;
- or pipeline must receive a validation failure;
- or pipeline must process segments directly.

Pipeline should not receive a stale `audio.m4a` without knowing it may be partial.

---

## Recommended Implementation Steps

1. Add shared `AudioArtifactValidationResult` helper.
2. Update `AudioAssetResolver.state(for:)` to check manifest first.
3. Add explicit legacy vs rendered-from-manifest states.
4. Remove or make unavailable the crashing `hasPlayableAudio` property.
5. Update `resolvePlayableURL(for:)` to render only after manifest validation.
6. Update `resolveExportableURL(for:)` to inspect `ConcatenationResult`.
7. Add logs for resolver decision:
   - manifest exists,
   - audio.m4a exists/size,
   - valid segment count,
   - selected source,
   - reason.

---

## Acceptance Tests

### Test 1 — Legacy audio only

Setup:

```text
no manifest
audio.m4a exists and size > 0
```

Expected:

- resolver returns legacy single file ready;
- playback/export uses audio.m4a.

---

### Test 2 — Manifest with valid segments, no audio.m4a

Setup:

```text
manifest exists
segment-000 exists size > 0
audio.m4a missing
```

Expected:

- resolver returns segments available / needs render;
- render produces validated audio;
- playback/export proceeds after successful render.

---

### Test 3 — Manifest with valid segments, stale audio.m4a

Setup:

```text
manifest exists with segment-000 and segment-001
audio.m4a exists but represents only segment-001
```

Expected:

- resolver does not return audio.m4a as ready;
- resolver requests regeneration or reports stale derived file;
- no pipeline/playback/export uses partial file as complete.

---

### Test 4 — Manifest missing segment-000 but audio.m4a exists

Setup:

```text
manifest expects segment-000 and segment-001
segment-000 missing
segment-001 exists
audio.m4a exists from segment-001
```

Expected:

- resolver returns manifest invalid / partial;
- does not silently return audio.m4a;
- error is visible in logs/UI/pipeline decision.

---

### Test 5 — Public resolver API cannot crash

Setup:

```text
call any public property/method on AudioAssetResolver
```

Expected:

- no `fatalError`;
- no app crash by design.

---

## Phase 4 Retest Gate

Do not proceed to pipeline/transcription tuning until:

- [ ] manifest is checked before `audio.m4a`;
- [ ] stale `audio.m4a` cannot hide valid segments;
- [ ] invalid manifest cannot be silently treated as unavailable;
- [ ] resolver distinguishes legacy audio from rendered manifest audio;
- [ ] render-on-demand uses `ConcatenationResult`;
- [ ] public resolver API has no intentional crash path.

---

## Summary

The resolver is the gateway used by playback, export, and potentially pipeline decisions.

If it trusts `audio.m4a` too early, every downstream layer can look correct while using incomplete audio.

The resolver must become manifest-first so the rest of the app cannot accidentally hide recording corruption.
