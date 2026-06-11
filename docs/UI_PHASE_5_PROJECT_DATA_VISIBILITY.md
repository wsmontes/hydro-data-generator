# UI Phase 5 — Project Data Visibility

Status: active
Parent audit: `docs/UI_DATA_VISIBILITY_EDITABILITY_AUDIT.md`
Previous phases:

- `docs/UI_PHASE_1_ITEM_DETAIL_VISIBILITY_MAP.md`
- `docs/UI_PHASE_2_USER_EDITING_PROTECTION_MODEL.md`
- `docs/UI_PHASE_3_TRANSCRIPT_ANALYSIS_REVIEW_UX.md`
- `docs/UI_PHASE_4_CONTEXT_TAGS_METADATA_RELATIONSHIPS.md`

Area: project home, project health, signals, tasks, items, recent activity, metrics, status, intention, and pending work
Last updated: 2026-06-10

This document audits how project-level information is shown, explained, edited, and acted on.

The central product question:

> Does the project screen help the user understand the state of a project and act on it, or does it only show a dashboard of derived numbers?

---

## 1. Product Rule

A project is not just a folder.

A project should show:

```text
what this project is about
what changed recently
what needs attention
what is blocked
what is pending review
what AI inferred
what the user accepted or rejected
what the next action should be
```

The user should be able to act directly from the project screen.

---

## 2. Current Situation

Based on the current `ProjectDetailView.swift`, the project entry delegates to `ProjectHomeView`.

Current visible project areas include:

- project header;
- editable project intention;
- project status badge;
- metric cards for items, tasks, and signals;
- health section;
- processing status;
- alerts/signals section;
- recent activity;
- add item actions;
- export JSON/Markdown;
- navigation to items, board, and signals.

This is a strong project dashboard foundation.

The issue is that several project-level signals are visible but not yet clearly reviewable, explainable, or editable.

---

## 3. Project Header

### Visible data

- project name;
- project intention;
- project status badge;
- AI badge on intention when auto-generated.

### Editable data

- project intention is editable inline;
- editing intention sets it as user-defined rather than auto-generated.

### What works well

This is one of the best current ownership patterns in the project UI.

The user can turn an AI/project placeholder into a user-defined intention.

### Gaps

Project name and status need equally clear editing flows.

The project header should expose:

```text
Edit Project Name
Edit Intention
Change Status
Archive / Restore Project
Project Settings
```

### Required ownership behavior

If the user edits project name, intention, or status:

```text
field becomes user-owned/protected
AI suggestions become reviewable changes
```

---

## 4. Project Status

### Current issue

Status is shown as a badge, but it is not clear whether the user can change it from the project home.

### Required states

At minimum:

```text
Active
Paused
Completed
Archived
Blocked / At Risk, if supported
```

### Required UX

```text
Status: Active
Actions: Change status
```

If AI suggests status changes, they should be suggestions:

```text
AI suggests this project may be at risk.
Review signal.
```

Do not silently change user-controlled project status.

---

## 5. Project Metrics

### Current visible metrics

The project screen shows cards for:

- Items;
- Tasks;
- Signals.

These cards are useful and navigable.

### Gap

Counts alone do not explain urgency.

Each metric should answer:

```text
What changed?
What needs action?
What is stale?
What is unreviewed?
```

### Recommended metrics

#### Items

```text
Total items
New/unreviewed items
Unprocessed items
Items with processing errors
Items added this week
```

#### Tasks

```text
Open tasks
In progress
Overdue
Blocked
Completed recently
```

#### Signals

```text
New signals
Critical signals
Unreviewed doubts
Accepted risks
Resolved signals
```

---

## 6. Project Health

### Current visible data

Project health section may show:

- health score;
- health status;
- ontology/inertia score;
- processing status.

### Problem

Health is a derived judgment. The UI must explain it.

The user needs to know:

```text
Why is health healthy/stale/at risk?
What evidence caused this?
What can I do?
Can I override it?
```

### Required UX

```text
Project Health
Status: At Risk
Reasons:
- 3 overdue tasks
- 2 unresolved critical doubts
- no new activity for 14 days
Actions: Review signals, Review overdue tasks, Mark as OK, Recalculate
```

### Ownership rule

If the user manually overrides project health/status, AI should not immediately overwrite it.

AI may suggest:

```text
AI still sees risk. Review evidence.
```

---

## 7. Signals Section

### Current good direction

Signals are surfaced in the project screen and have a dedicated navigation path.

### Gap

Signals need disposition.

A signal is not useful unless the user can do something with it.

### Required signal actions

```text
Accept
Dismiss
Resolve
Convert to Task
Convert to Risk
Link to source item
Show evidence
Ask AI to explain
```

### Required signal fields

```text
Signal type
Severity
Status
Source items
Created date
Last updated
Evidence
Suggested action
User decision
```

### Required states

```text
New
Needs review
Accepted
Dismissed
Resolved
Converted
Stale
```

---

## 8. Tasks Visibility

### Current situation

Project screen links to the task board and shows task counts.

### Gap

The project home should expose action-oriented task information, not just counts.

### Recommended task summary

```text
Tasks needing attention
- overdue
- due soon
- blocked
- unassigned
- created from AI suggestions but not reviewed
```

### Required actions

```text
Open task
Mark done
Change status
Create task
Review AI-suggested tasks
```

### AI action items

AI-generated action items should not automatically become trusted tasks unless product policy says so.

Safer MVP:

```text
AI suggests action item -> user reviews -> convert to task
```

---

## 9. Items Visibility

### Current situation

Project screen shows item counts and recent activity.

### Gap

The user needs to know which items require attention.

### Required item categories

```text
Recently added
Unreviewed
Unprocessed
Processing failed
Needs transcript correction
Needs analysis review
Has AI suggestions
```

### Recommended UI

```text
Items needing review
- Recording Jun 10 — Transcript ready, not reviewed
- Imported PDF — OCR needs correction
- Meeting note — 3 AI suggestions
```

Actions:

```text
Open
Mark reviewed
Process
Retry
Assign tags
```

---

## 10. Recent Activity

### Current situation

Project home has recent activity.

### Gap

Recent activity should distinguish raw activity from meaningful changes.

Recommended activity types:

```text
Item added
Task completed
Signal created
Signal resolved
Transcript corrected
Analysis accepted
Project status changed
Metadata edited
AI suggestion created
```

Each activity should show:

```text
who/what changed it
when
source item
action available
```

For local-first MVP, actor can be:

```text
User
AI
System
Import
```

---

## 11. Pending Work / Project Inbox

A project should have a clear pending work area.

Recommended project-level pending queue:

```text
Needs review
Needs processing
Needs correction
Needs decision
```

Examples:

```text
3 items need analysis review
2 signals need decision
1 transcript needs correction
4 suggested tags waiting review
```

This is more useful than passive dashboards.

---

## 12. Project-Level Editing

The project screen should support editing:

```text
Project name
Intention
Status
Priority
Tags
Description / notes
Review state
Archive state
```

Optional later:

```text
Stakeholders
Deadline
Outcome definition
Project area/domain
```

---

## 13. Project-Level Provenance

Project fields should show source/ownership:

```text
Intention: User defined
Health: AI calculated
Status: User set
Signal: AI suggested, unreviewed
Task: User created
Task: AI suggested, converted by user
```

This avoids confusion between user decisions and AI interpretation.

---

## 14. Project Export

Current export actions are useful.

But export should respect review/ownership state.

Project export should optionally include:

```text
accepted knowledge only
all generated artifacts
pending suggestions
review status
provenance metadata
```

Recommended export options:

```text
Export Project Summary
Export Full Project Archive
Export Reviewed Knowledge Only
Export Debug JSON
```

---

## 15. Recommended Project Home Layout

Suggested structure:

```text
1. Header
   - name, intention, status, edit actions

2. Attention Required
   - pending reviews, critical signals, overdue tasks

3. Project Health
   - status, reasons, actions

4. Metrics
   - items, tasks, signals, processing

5. Recent Activity
   - meaningful changes

6. Items / Tasks / Signals shortcuts
   - navigable sections

7. Project Actions
   - add item, record, import, export, settings
```

The most important change is moving from dashboard-first to attention-first.

---

## 16. Required Components

Recommended components:

```text
ProjectHeaderEditor
ProjectStatusPicker
ProjectAttentionSection
ProjectHealthExplanationSheet
SignalReviewCard
ProjectPendingReviewList
ProjectActivityTimeline
ProjectExportOptionsSheet
```

---

## 17. Acceptance Tests

### Test 1 — Edit project identity

Given a project:

1. user edits name/intention/status;
2. save succeeds;
3. fields show user-defined/protected state;
4. AI does not overwrite them directly.

### Test 2 — Health explanation

Given a project at risk:

1. user opens health details;
2. UI shows reasons and source evidence;
3. user can navigate to relevant tasks/signals/items.

### Test 3 — Signal review

Given a new project signal:

1. user can accept/dismiss/resolve/convert it;
2. signal status changes;
3. dismissed signal does not reappear as new without new evidence.

### Test 4 — Pending review

Given unreviewed items and suggestions:

1. project home shows pending review count;
2. user can open each item/suggestion;
3. completing review updates project attention section.

### Test 5 — Task attention

Given overdue tasks:

1. project home shows overdue count and top overdue tasks;
2. user can mark task done or open board;
3. health explanation reflects task state.

---

## 18. P0 Implementation Recommendations

### P0.1 Add Attention Required section

Show what needs action before passive metrics.

### P0.2 Add health explanation

Project health must explain itself.

### P0.3 Add signal disposition

Signals need accept/dismiss/resolve/convert actions.

### P0.4 Add project status editing

Status should be user-controllable and protected.

### P0.5 Show unreviewed/unprocessed item categories

Project should show what needs review, not only item count.

---

## 19. Summary

The project screen already has a strong dashboard base.

The next step is to make it operational.

A good project screen should answer:

```text
What is this project?
Is it healthy?
Why?
What changed?
What needs my attention?
What did AI suggest?
What have I accepted?
What should I do next?
```

That turns the project from a container into a working decision surface.
