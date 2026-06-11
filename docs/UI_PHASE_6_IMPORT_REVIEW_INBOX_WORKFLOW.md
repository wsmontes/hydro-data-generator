# UI Phase 6 — Import Review & Inbox Workflow

Status: active
Parent audit: `docs/UI_DATA_VISIBILITY_EDITABILITY_AUDIT.md`
Previous phases:

- `docs/UI_PHASE_1_ITEM_DETAIL_VISIBILITY_MAP.md`
- `docs/UI_PHASE_2_USER_EDITING_PROTECTION_MODEL.md`
- `docs/UI_PHASE_3_TRANSCRIPT_ANALYSIS_REVIEW_UX.md`
- `docs/UI_PHASE_4_CONTEXT_TAGS_METADATA_RELATIONSHIPS.md`
- `docs/UI_PHASE_5_PROJECT_DATA_VISIBILITY.md`

Area: imports, captures, inbox triage, review before processing, processing control, and user correction before AI analysis
Last updated: 2026-06-10

This document audits and defines the review workflow between capture/import and trusted knowledge.

The central product question:

> Does newly captured or imported information become trusted knowledge only after the user can see, correct, route, and review it?

---

## 1. Product Rule

Capture/import is not the same as acceptance.

Imported or captured data should pass through a clear lifecycle:

```text
Captured / Imported
-> Staged / Previewed
-> Saved to Inbox or Project
-> Processed / Extracted
-> Reviewed by User
-> Trusted Knowledge
```

The user should always know where an item is in this lifecycle.

---

## 2. Current Situation

The current home/import flow already supports many entry points:

- record audio;
- import audio;
- import text files;
- import PDFs / HTML / RTF / JSON / SRT / ICS / Anarlog / Meetily;
- import images;
- scan shared directory from share extension;
- stage single imports;
- extract metadata for audio;
- preview text snippet for text imports;
- assign imported item to a target project;
- enqueue processing after import.

This is a strong ingestion base.

The main UX gap is not import capability.

The main gap is review control.

The app needs to make it obvious whether the user has reviewed and accepted the extracted/generated data.

---

## 3. Import Lifecycle States

Recommended item lifecycle states:

```text
staged
importedUnreviewed
savedToInbox
assignedToProject
processingQueued
processingInProgress
processingFailed
extractionReadyForReview
analysisReadyForReview
reviewedTrusted
archived
```

These states can be implemented as explicit fields or derived from existing fields, but the UI needs to show them clearly.

---

## 4. Import Preview UX

Every import should show a preview before becoming trusted knowledge, especially when the app extracts or infers metadata.

Recommended preview fields:

```text
Original filename
Detected format
Suggested title
Detected date
Detected duration, for audio/video
Text snippet or OCR preview
Target project
Tags
Language
Process now toggle
Save to Inbox toggle
```

Recommended actions:

```text
Cancel
Save to Inbox
Save and Process
Assign to Project
```

---

## 5. Audio Import Review

Audio imports need special review because the user may not know what metadata was extracted.

Preview should show:

```text
Title
Original filename
Duration
Creation date, if available
Target project
Language / transcription locale
Auto-transcribe toggle
Auto-analyze toggle
```

After saving:

```text
Audio imported
Transcript pending / Processing queued / Waiting for review
```

If transcription fails, the item should remain in a recoverable review state, not appear as empty content.

---

## 6. Text Import Review

Text imports should show:

```text
Suggested title
Detected format
Text snippet
Full text length
Target project
Tags
Process now toggle
```

The user should be able to correct the title and optionally edit text before AI processing.

Recommended action:

```text
Review extracted text before processing
```

---

## 7. Image / OCR Import Review

Image imports and scans should make OCR state explicit.

Recommended flow:

```text
Image captured/imported
-> OCR extracted text
-> user can review/correct OCR
-> AI analysis runs from corrected OCR
```

Preview should show:

```text
Image thumbnail
Detected pages
OCR status
Extracted text snippet
Correct OCR action
Process now toggle
```

User-corrected OCR should become protected.

---

## 8. Share Extension Import Review

The share extension flow should avoid silently burying shared files.

After opening the app from share extension:

```text
Shared item received
Review import
```

The user should see:

```text
Source app/file
Detected type
Suggested title
Target project
Save to Inbox / Save and Process
```

If multiple shared files are pending, show a batch review list.

---

## 9. Inbox as Triage Surface

Inbox should not be just a list of unassigned items.

It should be a triage queue.

Recommended categories:

```text
Needs import review
Needs processing
Processing failed
Needs transcript review
Needs analysis review
Needs metadata review
Ready to file
```

Each item should show a clear next action.

Examples:

```text
Audio Recording — Transcript ready, needs review
PDF Import — OCR extracted, needs correction
Web Bookmark — Needs tags/project
Meeting Note — Analysis ready, 3 suggestions
```

---

## 10. Review Status vs Processing Status

These are different and should not be collapsed.

### Processing status

```text
queued
processing
completed
failed
```

### Review status

```text
not reviewed
needs correction
needs decision
reviewed
trusted
archived
```

A processed item can still be unreviewed.

A reviewed item can still have processing errors.

The UI should show both when relevant.

---

## 11. Process Now / Process Later

The user should be able to decide whether an item is processed immediately.

Settings may default to automatic processing, but item-level control is still useful.

Import review should offer:

```text
Process now
Save without processing
Process later
```

This is important for:

- sensitive content;
- large files;
- wrong metadata needing correction first;
- missing project assignment;
- wrong language selection.

---

## 12. Correction Before Analysis

The strongest flow is:

```text
Import/capture
-> review source/extracted text
-> correct metadata/transcript/OCR
-> then analyze
```

This avoids wasting AI processing on wrong input.

Examples:

```text
Correct language before transcription
Correct OCR before analysis
Correct transcript before summary
Assign project before project-level signal generation
```

---

## 13. Batch Import UX

For multiple files, the app should provide a batch review surface.

Recommended batch screen:

```text
5 files ready to import

[ ] agenda.pdf       PDF       Project: none       Action: Review
[ ] recording.m4a    Audio     Project: Elevator   Action: Review
[ ] notes.txt        Text      Project: Inbox      Action: Review

Actions: Import selected, Assign project, Add tags, Process now/off
```

Batch operations:

```text
assign project
add tag
process now
save to inbox
remove from batch
```

---

## 14. Import Error UX

Import errors should be actionable.

Instead of only:

```text
Import Error: failed
```

show:

```text
Could not import agenda.pdf
Reason: unsupported file / unreadable file / permission issue / extraction failed
Actions: Try again, Save file only, Cancel
```

If partial import succeeded, show what was preserved.

---

## 15. Processing Queue Visibility

The app has a processing queue concept.

The UI should expose enough of it for trust.

Recommended queue display:

```text
Processing Queue
- 2 queued
- 1 processing
- 1 failed
```

Per item:

```text
Queued for transcription
Queued for analysis
Failed: provider missing
Waiting: no active AI provider
```

Actions:

```text
Retry
Cancel
Open Settings
Process now
```

---

## 16. Inbox Item Actions

Each inbox/review item should support:

```text
Open
Review
Assign to project
Edit metadata
Edit extracted text/transcript/OCR
Process now
Retry processing
Mark reviewed
Archive
Remove
```

Avoid making the user open deep detail screens for every simple triage action.

---

## 17. Trust / Review Badges

Use consistent badges:

```text
Imported
Captured
Processing
Needs Review
Reviewed
Trusted
AI Generated
User Corrected
Protected
Processing Failed
```

These badges should appear in:

- Inbox list;
- Project item list;
- Item detail;
- Project attention section.

---

## 18. Recommended Components

```text
ImportReviewView
BatchImportReviewView
InboxTriageView
ProcessingQueueSummaryView
ImportErrorRecoveryView
ReviewStatusBadge
ImportMetadataEditor
```

---

## 19. Acceptance Tests

### Test 1 — Single audio import

1. user selects audio file;
2. app shows preview with duration/title/project/language;
3. user changes title and project;
4. user saves and processes;
5. item appears with processing and review status.

### Test 2 — Text import with correction before processing

1. user imports text file;
2. app shows snippet and suggested title;
3. user edits title/text before processing;
4. body text becomes user-protected;
5. analysis uses corrected text.

### Test 3 — Image import with OCR correction

1. user imports image;
2. OCR runs;
3. item appears as OCR needs review;
4. user corrects OCR;
5. analysis uses corrected OCR.

### Test 4 — Batch import

1. user selects multiple files;
2. batch review screen appears;
3. user assigns project/tag to all;
4. user saves all to inbox or process now;
5. each item gets correct review status.

### Test 5 — Processing failure

1. imported item processing fails;
2. item remains visible in inbox/project attention;
3. error reason is visible;
4. retry action is available.

### Test 6 — Processed but unreviewed

1. item finishes analysis;
2. item is still marked needs review;
3. user reviews transcript/summary;
4. item becomes reviewed/trusted.

---

## 20. P0 Implementation Recommendations

### P0.1 Add explicit review status

Separate review status from processing status.

### P0.2 Strengthen import preview

Every import should show suggested title/project/tags/process choice.

### P0.3 Add inbox triage categories

Group by next action, not just date.

### P0.4 Allow correction before analysis

Especially for text, OCR, transcript, language, and project assignment.

### P0.5 Show processing failures in inbox/project attention

Failures must remain visible and recoverable.

---

## 21. Summary

The import system is already capable.

The next UX step is to make import review explicit.

The app should not silently transform raw captures/imports into trusted AI knowledge.

The better flow is:

```text
bring data in
show what was detected
let the user correct context
process with the right input
show generated output as reviewable
promote reviewed information into trusted knowledge
```

That is how wawa-note becomes a user-controlled knowledge system instead of a black-box importer.
