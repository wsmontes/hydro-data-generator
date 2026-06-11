# UI Phase 1 — Item Detail Visibility Map

Status: active
Parent audit: `docs/UI_DATA_VISIBILITY_EDITABILITY_AUDIT.md`
Area: `KnowledgeDetailView`
Last updated: 2026-06-10

This document maps what the user can see and edit on the item detail screen.

The goal is to determine whether each meaningful piece of item data is:

```text
visible
understandable
editable
reviewable
protected
recoverable
```

---

## 1. Screen Under Audit

Primary file:

```text
wawa-note/UI/Knowledge/KnowledgeDetailView.swift
```

Supporting files already observed:

```text
wawa-note/UI/Knowledge/NoteEditorView.swift
wawa-note/UI/Project/ProjectDetailView.swift
wawa-note/UI/Home/HomeView.swift
```

---

## 2. Current Detail Screen Structure

Based on the current `KnowledgeDetailView`, the item detail page includes these sections:

```text
1. Header
2. Processing / pipeline status
3. Transcription error display
4. Audio player / audio preparation
5. Transcript and analysis artifact sections
6. Image / OCR section
7. Body text section
8. Web bookmark section
9. Context metadata section
10. Developer raw response section
11. Annotations
12. Backlinks
13. Toolbar actions
14. Export menu
```

This is a strong base. The screen is information-rich.

The issue is not lack of displayed data. The issue is uneven user agency.

---

## 3. Section-by-Section Map

## 3.1 Header

### Visible data

- item title
- item type icon
- item type label
- created date/time
- duration, when present
- mood badge for journal entries
- status badges

### Editable data

- title is editable only inside the current edit mode
- edit mode appears only when `item.bodyText != nil`

### Problem

Title is universal item identity, but the Edit button is conditional.

This means some items may not expose title editing from detail view:

- audio-only recordings
- imported files without body text
- web bookmarks
- image items
- analysis-only items
- items whose main content is transcript/artifacts

### Required change

Title editability must not depend on `bodyText`.

Every item type should allow title editing.

### Recommended UI

```text
Header
- title
- tap title or Edit Identity
- title provenance badge: User / AI / Imported / System
```

---

## 3.2 Processing / pipeline status

### Visible data

- active processing state
- transcription progress
- pipeline stage
- agent thinking status
- number of agent steps
- agent event badges

### Editable data

None.

### Problem

Processing status is visible, but not actionable enough.

The user may need to:

- retry;
- cancel;
- inspect failure;
- see which source artifact is being processed;
- know whether processing is using transcript, body text, OCR, audio, or stale artifact.

### Required change

Processing banner should become actionable when stalled/failed.

### Recommended UI

```text
Processing...
Stage: Transcribing audio
Input: segment manifest / audio.m4a / OCR text / bodyText
Actions: Cancel, Retry, View details
```

---

## 3.3 Transcription error display

### Visible data

- transcription error message
- Open Settings button when relevant

### Editable data

None.

### Problem

Transcription error is shown as an error string, but it does not distinguish:

```text
permission issue
provider issue
artifact issue
empty result despite valid audio
partial result
missing audio
```

### Required change

Transcription errors need categories and recovery actions.

### Recommended UI

```text
Transcript not ready
Reason: Audio artifact issue / Provider error / Permission issue
Actions: Retry transcription, View audio artifacts, Open Settings
```

---

## 3.4 Audio player / audio preparation

### Visible data

- playable audio if resolver provides URL
- preparing state
- button to prepare audio from segments
- failed audio state reason

### Editable data

None.

### Problem

Playback exists, but artifact health is not visible to normal users.

The user does not clearly see:

- whether audio comes from single file or manifest;
- whether segments exist;
- whether `audio.m4a` was derived;
- whether audio is partial;
- whether the transcript used the same audio.

### Required change

Audio status should be visible at least when there is a problem.

### Recommended UI

```text
Audio
Status: Ready / Preparing / Needs repair / Partial
Source: Original recording / Rendered from 3 segments / Imported file
Actions: Play, Export, Prepare, Retry repair, View details
```

---

## 3.5 Transcript section

### Visible data

Transcript is shown when artifact exists.

### Editable data

Not clearly editable.

### Problem

Transcript is central user information but currently behaves like a generated artifact.

A transcript must be correctable. Otherwise the user cannot trust the downstream analysis.

### Required change

Transcript must become a first-class editable/reviewable section.

### Recommended UI

```text
Transcript
Badge: AI transcribed / User corrected / Protected
Actions: Edit transcript, Re-transcribe, Re-analyze from transcript, Show original
```

### Required ownership behavior

When user edits transcript:

```text
transcript origin = user
transcript protected = true
future transcription results become suggestions
analysis should use corrected transcript
```

---

## 3.6 Analysis / summary section

### Visible data

Analysis is shown when available.

Likely includes:

- summary
- action items
- key points
- risks / doubts / signals
- structured AI output

### Editable data

Not clearly editable.

### Problem

AI analysis appears as output, not as reviewable claims.

The user needs to decide what becomes trusted knowledge.

### Required change

Analysis should support field-level review:

```text
accept
edit
reject
convert
resolve
protect
```

### Recommended UI

```text
Summary
Badge: AI generated
Actions: Accept, Edit, Re-run

Action Items
Actions: Convert to Task, Ignore, Edit

Doubts / Risks
Actions: Resolve, Convert to Signal, Dismiss
```

---

## 3.7 Image / OCR section

### Visible data

- scanned images
- OCR text inside image section

### Editable data

Not clearly editable in the same way as body text.

### Problem

OCR is generated text and often needs correction.

If image OCR becomes `bodyText`, users need to know whether they are editing OCR, body text, or original source.

### Required change

OCR text should have explicit correction UX.

### Recommended UI

```text
Extracted text from image
Badge: OCR generated
Actions: Correct OCR, Re-run OCR, Use as body text, Protect correction
```

---

## 3.8 Body text section

### Visible data

- body text for notes, journals, and non-image items with `bodyText`

### Editable data

Yes, via detail edit mode and/or `NoteEditorView`.

### What works well

`NoteEditorView` shows a mature pattern:

- title editing;
- body editing;
- save/cancel;
- keyboard toolbar;
- provenance marking for user-created/user-edited fields;
- processing enqueue after content creation.

### Problem

This good pattern is not generalized to other sections.

### Required change

Use the NoteEditor ownership model as the pattern for:

- transcript;
- OCR;
- summary;
- context metadata;
- project intention;
- extracted action items.

---

## 3.9 Web bookmark section

### Visible data

A bookmark section exists for web bookmark items.

### Editable data

Unknown / not clearly first-class.

### Problem

Bookmarks often have user-correctable metadata:

- title;
- URL;
- description;
- excerpt;
- tags;
- project;
- why it was saved.

### Required change

Bookmark metadata should be editable.

### Recommended UI

```text
Bookmark
URL
Title
Description / note
Tags
Open link
Edit metadata
```

---

## 3.10 Context metadata section

### Visible data

Context fields are displayed read-only when present.

### Editable data

No.

### Problem

Context metadata is user-correctable by nature.

Examples:

- date;
- source;
- project;
- item type;
- language;
- tags;
- location;
- import metadata;
- participants;
- meeting type.

### Required change

Add `Edit Metadata` / `Context Editor`.

### Recommended UI

```text
Context
Date
Source
Project
Tags
Language
Participants
Actions: Edit metadata, Move to project, Change type
```

---

## 3.11 Developer raw response section

### Visible data

Raw LLM response is shown only in developer mode and only under narrow conditions.

### Editable data

No.

### Problem

Developer raw JSON is not a user-facing solution for transparency.

Normal users need understandable provenance/evidence, not raw JSON.

### Required change

Keep raw JSON developer-only, but add user-facing explanation affordances:

```text
Why this summary?
Sources used
Generated from transcript at time X
Model/provider used
```

---

## 3.12 Annotations

### Visible data

Annotations are shown when present.

### Editable data

Unknown / not clearly visible from current map.

### Problem

Annotations should be user-owned knowledge.

### Required change

Annotations need:

- add;
- edit;
- delete;
- link to source span;
- mark as user note / AI suggestion.

---

## 3.13 Backlinks / relationships

### Visible data

Backlinks are shown.

There is also a `Connect to Item` action that creates bidirectional graph edges.

### Editable data

Creation exists.

Management is unclear.

### Problem

Relationships are important knowledge graph data.

Users need to:

- see why items are connected;
- edit relationship type;
- remove bad links;
- accept/reject AI-suggested links;
- see provenance of relationship.

### Required change

Add relation manager.

### Recommended UI

```text
Connections
Related to: Item A
Type: relatesTo / supports / contradicts / follows-up / source-for
Source: User / AI / Import
Actions: Edit, Remove, Explain
```

---

## 3.14 Toolbar actions

### Current actions

The toolbar includes:

- Edit, only when `item.bodyText != nil`
- Turn into Project
- Connect to Item
- Re-analyze
- Export

### Problems

1. Edit is conditionally hidden.
2. Re-analyze appears under certain provider/processing conditions but user may not understand what input it uses.
3. Export is useful but does not replace editing/review.
4. There is no obvious metadata/tag/review action.

### Required toolbar changes

Replace generic conditional toolbar with item-wide actions:

```text
Edit Title
Edit Content / Transcript / OCR depending on item type
Manage Tags
Edit Metadata
Review AI Output
Reprocess
Export
Connect
Promote to Project
```

---

## 4. Visibility and Editability Table

| Section | Visible | Editable | Ownership visible | Main issue |
|---|---:|---:|---:|---|
| Header/title | Yes | Sometimes | No | Edit tied to bodyText |
| Created date/duration | Yes | No | N/A | Metadata correction missing |
| Audio | Yes | No | No | Artifact health hidden |
| Transcript | Yes if exists | No | No | Cannot correct transcript |
| Analysis | Yes if exists | No | No | Cannot accept/edit/reject |
| OCR | Yes for images | Not clear | No | OCR correction missing |
| Body text | Yes | Yes for body items | Partly | Good pattern not generalized |
| Bookmark | Yes | Not clear | No | Bookmark metadata editing missing |
| Context metadata | Yes | No | No | Read-only even when wrong |
| Annotations | Yes if exists | Not clear | No | Annotation ownership unclear |
| Backlinks | Yes | Creation only | No | Relationship management missing |
| Raw JSON | Developer only | No | N/A | Not user-facing transparency |
| Export | Yes | N/A | N/A | Does not solve ownership |

---

## 5. Immediate Fix List

### P0 — Must fix for data ownership

- Make title editable for every item type.
- Add transcript correction UI.
- Add summary edit/accept/reject UI.
- Add visible provenance badges: AI generated, User edited, Protected.
- Add metadata editor.
- Replace silent save failures with visible errors.

### P1 — Should fix soon

- Add tag manager.
- Add relationship manager.
- Add OCR correction flow.
- Add artifact status view for audio/transcript/analysis.
- Add reprocess input selector: reprocess from audio, transcript, body text, OCR, or corrected content.

### P2 — Nice but important for trust

- Show AI model/provider used.
- Show why summary/signal/action item was generated.
- Show source spans/evidence.
- Show field edit history.
- Support diff between AI version and user-edited version.

---

## 6. Recommended Item Detail Architecture

Move toward section-level models:

```swift
struct ItemDetailSectionState {
    let id: String
    let title: String
    let source: DataOrigin
    let ownership: FieldOwnership
    let editability: Editability
    let status: SectionStatus
    let actions: [SectionAction]
}
```

Example enums:

```swift
enum DataOrigin {
    case user
    case imported
    case audioTranscription
    case ocr
    case aiGenerated
    case system
}

enum FieldOwnership {
    case unowned
    case aiGenerated
    case userEditedProtected
    case aiSuggestedChange
}

enum Editability {
    case readOnly
    case inlineEditable
    case sheetEditable
    case reviewOnly
}
```

The screen should not hard-code editability only around `bodyText`.

---

## 7. Recommended User Flow

### Current implied flow

```text
Capture/import -> process -> show result
```

### Desired ownership flow

```text
Capture/import
-> show original
-> show extracted/generated data
-> user can correct/review
-> user edits are protected
-> AI suggestions become reviewable changes
-> trusted knowledge is updated
```

---

## 8. Acceptance Tests

### Test 1 — Audio recording item

Given an audio item with transcript and analysis:

- user can edit title;
- user can correct transcript;
- user can re-run analysis from corrected transcript;
- user can see audio status;
- user can export audio;
- user can see which fields are AI-generated.

### Test 2 — Imported image item

Given an image item with OCR:

- user can view image;
- user can correct OCR;
- correction is protected;
- analysis uses corrected OCR;
- original OCR can be retained for history/debug.

### Test 3 — Note item

Given a note item:

- current edit flow still works;
- title and body edits mark provenance;
- reprocess warns about protected edits;
- AI does not overwrite edited body silently.

### Test 4 — Project-related item

Given an item linked to a project:

- user can see project context;
- user can move/remove project link;
- user can manage relationships/backlinks;
- user can see whether relation was user-created or AI-generated.

### Test 5 — AI-generated analysis

Given a generated summary/action item:

- user can accept summary;
- user can edit summary;
- user can reject summary;
- user can convert action item to task;
- edited summary becomes protected.

---

## 9. Next Audit Step

Proceed to:

```text
UI Phase 2 — User editing and protection model
```

Focus files:

```text
NoteEditorView.swift
KnowledgeDetailView.swift
ProjectDetailView.swift
field provenance model/service files
ContentPipelineService.swift
```

Questions:

- Where is provenance enforced?
- What fields can AI overwrite?
- What happens when user edits a generated field?
- Does reprocess actually preserve protected edits?
- Are AI changes converted into suggestions, or do they overwrite?

---

## 10. Summary

`KnowledgeDetailView` is already a strong display surface.

But it is not yet a strong ownership surface.

The next product milestone should be:

> Every important field shown on item detail must have a clear source, editability status, and protection behavior.
