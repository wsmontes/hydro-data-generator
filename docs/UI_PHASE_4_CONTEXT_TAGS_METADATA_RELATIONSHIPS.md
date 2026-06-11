# UI Phase 4 — Context, Tags, Metadata & Relationships

Status: active
Parent audit: `docs/UI_DATA_VISIBILITY_EDITABILITY_AUDIT.md`
Previous phases:

- `docs/UI_PHASE_1_ITEM_DETAIL_VISIBILITY_MAP.md`
- `docs/UI_PHASE_2_USER_EDITING_PROTECTION_MODEL.md`
- `docs/UI_PHASE_3_TRANSCRIPT_ANALYSIS_REVIEW_UX.md`

Area: item context, tags, metadata, project assignment, item type, source data, and graph relationships
Last updated: 2026-06-10

This document defines the UI requirements for the information around the content: metadata, tags, project context, source, and relationships.

The central product question:

> Can the user put information in the right place, correct wrong context, and control how items relate to each other?

---

## 1. Product Rule

Content is not enough.

A knowledge item becomes useful when the user can control its context:

```text
What is this?
Where did it come from?
When did it happen?
Which project does it belong to?
What tags describe it?
What does it relate to?
Why is it connected?
```

The UI must make these fields visible, editable, and protected when user-corrected.

---

## 2. Current Situation

Confirmed patterns from the current UI:

- `KnowledgeDetailView` has a context metadata section, but it is presented as read-only.
- `KnowledgeDetailView` has `Connect to Item`, which creates item relationships.
- `ProjectHomeView` lets the user record, add notes, import files, and add content into a project context.
- `HomeView` import flow can assign imported content to a target project.
- `NoteEditorView` can create an item with initial tags.
- Journal mood appears as a `mood/` tag.

The concepts exist. The missing part is a consistent user-facing management layer.

---

## 3. Required Context Model

Each item should expose an editable context panel.

Suggested fields:

```text
Item type
Title
Created date
Event date
Source type
Source file / URL / capture method
Project assignment
Tags
Language / locale
Participants / people
Location
Related items
Inbox / archive / review status
Processing status
Provenance / ownership
```

Not every field applies to every item, but the user should have a predictable place to review and correct context.

---

## 4. Context Section UX

The context section should not be read-only.

It should show the current metadata and offer an edit action:

```text
Context
Type: Audio Recording
Project: Elevator Modernization
Date: Jun 10, 2026
Source: Recorded in app
Tags: meeting, strata, elevator
Language: English
Actions: Edit Metadata
```

When the user edits metadata, the field becomes user-owned/protected.

Future AI/import updates should become suggestions, not direct overwrites.

---

## 5. Metadata Editor

Create a dedicated editor sheet:

```text
MetadataEditorView(item: KnowledgeItem)
```

Initial fields:

```text
Title
Item Type
Project
Date
Tags
Language
Source
Inbox / archive / review status
```

Save behavior:

- user-edited metadata becomes protected;
- AI/import future updates become suggestions;
- save errors are visible;
- user-facing saves should not silently use `try?`.

---

## 6. Tags UX

Tags should be first-class.

Create:

```text
TagManagerSheet
```

Capabilities:

- add tag;
- remove tag;
- choose existing tag;
- show tag origin: user / AI / imported / system;
- accept or reject AI-suggested tags;
- protect user-added tags from automatic removal.

Examples:

```text
meeting        User
mood/happy     User/System journal flow
elevator       AI suggested
import/pdf     System
```

---

## 7. Project Assignment UX

Each item should support:

```text
Move to Project
Add to Project
Remove from Project
Change Primary Project
Create Project from Item
```

Current `Turn into Project` is useful, but it does not replace explicit project assignment management.

Recommended UI:

```text
Project
Elevator Modernization
Actions: Change, Remove, Add another project
```

If the user manually assigns an item to a project, AI should not remove that assignment directly.

AI may suggest additional project links.

---

## 8. Item Type UX

The user should be able to correct item type when misclassified.

Examples:

```text
Imported text -> Meeting note
Image -> Receipt / document / whiteboard
Audio -> Meeting / voice memo / interview
Web bookmark -> Research item
```

Changing item type should:

- preserve original source;
- not delete artifacts;
- update visible sections;
- optionally trigger re-analysis;
- mark type as user-protected.

---

## 9. Date and Time UX

The app should distinguish:

```text
createdAt: when item was created in app
sourceCreatedAt: file/source creation date
eventDate: when the meeting/event/content actually happened
updatedAt: last app update
processedAt: last AI processing time
```

User-facing date should usually be event/source date, not always app creation date.

If user edits event date, protect it.

---

## 10. Source UX

The user should know where an item came from.

Examples:

```text
Recorded in app
Imported audio file
Share extension
PDF import
Image scan
Manual note
Web bookmark
Calendar import
Meetily import
Anarlog import
```

Recommended UI:

```text
Source
Type: PDF Import
Original filename: agenda.pdf
Imported at: date
Actions: View source details, Export original if available, Edit source metadata
```

If the original file is not retained, the UI should not imply it is available.

---

## 11. Language / Locale UX

Language affects transcription and analysis quality.

Required UI:

```text
Language
Detected: Portuguese (Brazil)
Actions: Change language, Re-transcribe, Re-analyze
```

Changing language should offer downstream actions:

```text
Re-transcribe with this language?
Re-analyze existing transcript only?
```

---

## 12. People / Participants UX

Meeting notes and recordings often need participants.

Future field:

```text
Participants
```

MVP can keep this simple as text chips:

- add person manually;
- mark participant as confirmed;
- protect user corrections.

---

## 13. Relationship / Backlink UX

`Connect to Item` is a good start, but creation is not enough.

Users need to:

- view relation type;
- change relation type;
- remove relation;
- see why two items are connected;
- see provenance: user-created or AI-suggested;
- accept/reject AI-suggested relationships.

Start with a small relation type set:

```text
Related
Supports
Contradicts
Follow-up
Source
```

Relationship card example:

```text
Connected item: Board Meeting Notes
Relationship: Supports
Origin: User created
Actions: Edit, Remove, Explain
```

---

## 14. Inbox / Archive / Review Status UX

Review status should be separate from processing status.

Example:

```text
Processing: complete
Review: not reviewed
```

Recommended item actions:

```text
Keep in Inbox
Mark Reviewed
Move to Project
Archive
Remove
```

---

## 15. Metadata Suggestions UX

AI/import may suggest metadata:

```text
suggested tags
suggested project
suggested date
suggested item type
suggested relationships
suggested participants
```

If those suggestions conflict with user-protected values, they must appear as suggestions.

Example:

```text
Suggested metadata updates
+ tag: elevator
+ project: Strata Council
Change type: Note -> Meeting
Actions: Accept all, Review, Dismiss
```

---

## 16. Required Components

Recommended components:

```text
MetadataEditorView
TagManagerSheet
ProjectAssignmentSheet
RelationshipManagerView
SourceInfoView
MetadataSuggestionReviewSheet
```

---

## 17. Acceptance Tests

### Test 1 — Edit metadata

Given any item:

1. user opens Edit Metadata;
2. changes date and item type;
3. save succeeds;
4. fields show User edited / Protected;
5. reprocess does not overwrite them.

### Test 2 — Manage tags

Given AI-suggested tags:

1. user accepts one tag;
2. rejects another;
3. adds manual tag;
4. manual tag is protected.

### Test 3 — Move item to project

Given inbox item:

1. user moves item to a project;
2. inbox state updates;
3. project assignment is visible on item detail;
4. AI cannot remove assignment directly.

### Test 4 — Correct item type

Given misclassified imported item:

1. user changes type;
2. UI sections update;
3. original source remains available;
4. type is protected.

### Test 5 — Manage relationship

Given two connected items:

1. user sees connection;
2. changes relation type;
3. removes relation;
4. save errors are visible;
5. relation provenance is shown.

---

## 18. P0 Implementation Recommendations

### P0.1 Add Edit Metadata

Make metadata editing available from every item detail.

### P0.2 Add Tag Manager

Tags are too important to remain implicit.

### P0.3 Add Project Assignment Sheet

Move/add/remove item from project explicitly.

### P0.4 Add Relationship Manager

Current `Connect to Item` should be complemented by edit/remove/relation type management.

### P0.5 Show source/provenance badges

Show whether context came from user, import, AI, OCR, transcription, or system.

---

## 19. Summary

wawa-note should not only capture content.

It should let users organize and correct context.

The metadata layer is where information becomes useful:

```text
the right item
in the right project
with the right date
with the right tags
connected to the right things
under user control
```

That is the difference between a pile of captured notes and a working knowledge system.
