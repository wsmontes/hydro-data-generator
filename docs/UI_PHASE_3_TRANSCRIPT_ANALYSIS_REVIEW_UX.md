# UI Phase 3 — Transcript & Analysis Review UX

Status: active
Parent audit: `docs/UI_DATA_VISIBILITY_EDITABILITY_AUDIT.md`
Previous phases:

- `docs/UI_PHASE_1_ITEM_DETAIL_VISIBILITY_MAP.md`
- `docs/UI_PHASE_2_USER_EDITING_PROTECTION_MODEL.md`

Area: transcript, summary, action items, doubts, risks, signals, and AI review flow
Last updated: 2026-06-10

This document defines the review and editing UX for transcript and AI analysis artifacts.

The central product question:

> Does the user review and own AI-generated knowledge, or does the app silently treat AI output as truth?

---

## 1. Product Rule

Transcript and analysis are not final truth.

They are generated artifacts until the user accepts, edits, rejects, or relies on them.

The UI should make this explicit.

Recommended mental model:

```text
Original capture -> generated transcript -> user-corrected transcript -> generated analysis -> user-reviewed knowledge
```

---

## 2. Current Situation

From the current item detail flow, transcript and analysis are loaded as artifacts and shown when available.

This is good for visibility.

However, the UX still appears to treat these artifacts mostly as display output.

Missing product concepts:

- transcript correction;
- corrected transcript as source of truth;
- AI summary accept/edit/reject;
- action item conversion/review;
- doubt/risk resolution;
- field-level protection;
- suggestion review when AI disagrees with user edits;
- evidence/source trace for AI claims.

---

## 3. Required Section Model

Transcript and analysis sections should not be plain render blocks.

They should be reviewable cards with state.

Suggested model:

```swift
struct ReviewableSectionState {
    let id: String
    let title: String
    let origin: DataOrigin
    let reviewStatus: ReviewStatus
    let protectionStatus: ProtectionStatus
    let sourceArtifacts: [String]
    let actions: [ReviewAction]
}

enum ReviewStatus {
    case generatedNotReviewed
    case accepted
    case userEdited
    case rejected
    case needsReview
    case failed
}

enum ProtectionStatus {
    case unprotected
    case protectedByUser
    case aiSuggestionAvailable
}
```

---

## 4. Transcript UX

## 4.1 Transcript states

Transcript should support these states:

```text
No transcript
Transcribing
AI transcript ready, not reviewed
User corrected transcript
Transcript failed
Transcript stale after audio change
AI transcript suggestion available
```

---

## 4.2 Transcript display

Transcript header should show:

```text
Transcript
Badge: AI transcribed / User corrected / Protected / Needs review
Source: audio.m4a / segments / imported transcript
Timestamp: generated or edited time
```

---

## 4.3 Transcript actions

Required actions:

```text
Edit transcript
Accept transcript
Re-transcribe
Re-analyze from transcript
Show original transcript
Compare with AI suggestion
Export transcript
```

Not all need to appear at once. Use a menu if needed.

---

## 4.4 Transcript correction flow

When user taps `Edit transcript`:

1. open transcript editor sheet;
2. show current active transcript;
3. allow editing as text;
4. optionally allow segment/time markers later;
5. on Save:
   - store corrected transcript;
   - mark transcript as user-edited/protected;
   - preserve original transcript;
   - offer `Re-analyze now`.

Suggested save result:

```text
Transcript corrected. Re-analyze from corrected transcript?
```

---

## 4.5 Original vs corrected transcript

Do not destroy the original generated transcript.

Recommended artifact layout:

```text
transcript.original.json
transcript.json                 // active transcript
transcript.user-corrected.json   // optional explicit corrected artifact
transcript.suggestion.json       // future AI retranscription suggestion
```

MVP can store fewer files, but conceptually there must be:

```text
active transcript
original transcript or history
provenance/protection
```

---

## 4.6 Re-transcription behavior

If transcript is user-protected, re-transcription must not overwrite active transcript.

Instead:

```text
AI created a new transcript suggestion.
Review suggestion.
```

Actions:

```text
Accept replacement
Compare
Merge manually
Reject
```

---

## 5. Summary UX

## 5.1 Summary states

Summary should support:

```text
No summary
AI summary generated, not reviewed
Accepted summary
User edited summary
Rejected summary
AI suggestion available
Stale summary after transcript correction
```

---

## 5.2 Summary display

Header:

```text
Summary
Badge: AI generated / Accepted / User edited / Protected / Needs review
Generated from: transcript/bodyText/OCR/import
```

---

## 5.3 Summary actions

Required actions:

```text
Accept
Edit
Reject
Re-generate
Compare with previous
Show source evidence
```

---

## 5.4 Summary edit behavior

When user edits summary:

```text
summary becomes user-owned/protected
future re-analysis creates suggestion, not overwrite
```

---

## 5.5 Stale summary behavior

If transcript/body/OCR source changes after summary generation:

```text
Summary may be out of date
Actions: Re-analyze, Keep as-is, Compare after re-analysis
```

Do not silently regenerate over a protected summary.

---

## 6. Key Points / Topics UX

If analysis extracts key points, topics, or decisions, each should be separately reviewable.

Recommended card actions:

```text
Accept
Edit
Remove
Convert to note
Link to project/task/signal
Show evidence
```

Each key point should have:

```text
source text / transcript span
origin = AI generated or user edited
review status
```

---

## 7. Action Items UX

Action items are not just text. They are candidates for tasks.

## 7.1 Action item states

```text
suggested
accepted
convertedToTask
dismissed
edited
```

## 7.2 Required actions

```text
Convert to Task
Edit before converting
Assign project
Set due date
Dismiss
Show evidence
```

## 7.3 Conversion behavior

When user converts an AI action item to a task:

```text
task becomes user-created/user-accepted
link task back to source item and analysis artifact
future re-analysis cannot mutate the task directly
AI may create a new suggested update
```

---

## 8. Doubts, Risks, Contradictions, and Signals UX

AI-detected doubts/risks/signals should not be treated as confirmed facts.

They should be reviewable signals.

## 8.1 Signal states

```text
new
accepted
dismissed
resolved
convertedToTask
convertedToProjectRisk
needsEvidence
```

## 8.2 Required actions

```text
Accept
Dismiss
Resolve
Convert to Task
Convert to Project Signal
Show source evidence
Ask AI to explain
```

## 8.3 Evidence requirement

Every signal should answer:

```text
Why am I seeing this?
Which item/transcript passage caused this?
What can I do with it?
```

Without evidence, the UI should label it as weak/needs review.

---

## 9. Analysis Review Dashboard

For long recordings or complex items, the detail view can become crowded.

Recommended pattern:

```text
Analysis Review
- Summary
- Key points
- Action items
- Doubts/Risks
- Suggestions
- Evidence
```

Each row has status:

```text
New
Accepted
Edited
Dismissed
Protected
```

This can start as a section inside item detail, then become a sheet later.

---

## 10. Field-Level Suggestion UX

When AI proposes a change to a protected field, the user should see:

```text
AI suggested an update to your protected summary.
```

Actions:

```text
Accept
Reject
Compare
Merge
```

Comparison view:

```text
Current user version
AI suggested version
Reason/source
```

This applies to:

- transcript;
- summary;
- action item text;
- tags;
- metadata;
- project intention;
- task description.

---

## 11. Re-analysis UX

Current reprocess UI warns about protected edits.

That warning should become more specific.

Recommended re-analysis sheet:

```text
Re-analyze item

Input:
[x] Corrected transcript
[ ] Original transcript
[ ] Audio re-transcription
[ ] Body text
[ ] OCR text

Protected fields:
- Transcript: protected
- Summary: protected
- Title: protected

Output behavior:
AI will create suggestions for protected fields.
```

Actions:

```text
Cancel
Re-analyze
```

---

## 12. Empty / Failed Analysis UX

If transcript exists but analysis is missing:

```text
Transcript ready
Analysis not generated
Action: Generate analysis
```

If analysis failed:

```text
Analysis failed
Reason: provider/artifact/input issue
Actions: Retry, View details, Change model
```

If transcript corrected after analysis:

```text
Analysis may be out of date
Action: Re-analyze from corrected transcript
```

---

## 13. Required Data Model Support

Transcript/analysis review UX needs these concepts in data or artifacts:

```text
active artifact
original artifact
review status
protection status
source artifact
generatedAt / editedAt
suggestions
field-level provenance
```

Minimal MVP fields could live in sidecar files:

```text
transcript.review.json
analysis.review.json
suggestions.json
```

Example:

```swift
struct ArtifactReviewState: Codable {
    let artifactName: String
    let status: String
    let isProtected: Bool
    let sourceArtifact: String?
    let lastReviewedAt: Date?
    let lastEditedAt: Date?
}
```

---

## 14. UI Components to Build

### TranscriptReviewSection

Responsibilities:

- show transcript;
- show provenance/review badges;
- edit transcript;
- accept transcript;
- re-transcribe;
- compare suggestion.

### AnalysisReviewSection

Responsibilities:

- show summary;
- show action items;
- show signals/doubts;
- show review status;
- route actions to editors/converters.

### FieldSuggestionReviewSheet

Responsibilities:

- compare current vs suggested value;
- accept/reject/merge;
- update provenance.

### ReanalysisOptionsSheet

Responsibilities:

- select input source;
- show protected fields;
- explain output behavior.

---

## 15. Acceptance Tests

### Test 1 — Correct transcript then re-analyze

Given an audio item with transcript and analysis:

1. user edits transcript;
2. transcript badge becomes User corrected / Protected;
3. user runs re-analysis from corrected transcript;
4. new analysis uses corrected transcript;
5. transcript is not overwritten.

### Test 2 — Re-transcribe protected transcript

Given a protected transcript:

1. user taps re-transcribe;
2. AI transcript result is stored as suggestion;
3. active transcript remains user version;
4. user can compare/accept/reject.

### Test 3 — Edit summary then re-analyze

Given AI summary:

1. user edits summary;
2. summary becomes protected;
3. re-analysis creates summary suggestion;
4. active user summary remains unchanged.

### Test 4 — Convert action item to task

Given AI action item:

1. user edits action item text;
2. user converts to task;
3. task is linked to source item;
4. future AI analysis does not mutate task directly.

### Test 5 — Dismiss bad signal

Given an AI signal:

1. user dismisses signal;
2. signal status becomes dismissed;
3. same signal is not re-shown as new immediately after re-analysis unless new evidence exists.

---

## 16. P0 Implementation Recommendations

Start small.

### P0.1 Transcript correction

Add edit transcript flow and protection.

### P0.2 Summary edit/accept/reject

Add summary-level review controls.

### P0.3 Action item conversion

Let user convert AI action items into tasks with source link.

### P0.4 Suggestion storage

When protected fields conflict with new AI output, store suggestion instead of overwrite.

### P0.5 Re-analysis options

Re-analysis should explicitly use corrected transcript/body/OCR.

---

## 17. Summary

The app should not ask users to trust AI output blindly.

Transcript and analysis need to become reviewable knowledge layers:

```text
AI generates
User reviews
User corrections are protected
Future AI output becomes suggestion
Trusted knowledge emerges from the review process
```

This is the UI layer where wawa-note becomes more than a transcription app.
