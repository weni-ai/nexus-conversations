# Data Model: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md) | **Date**: 2026-07-28

## Entity: Conversation (extended)

Table: `intelligences_conversation` (`conversation_ms.models.Conversation`).

`resolution` remains the business outcome (`0`–`4`). Pipeline fields are control plane only.

### Stage vocabulary

Stored strings: `pending`, `done`, `skipped`, `failed`. Absence = SQL `NULL` (never the string `"null"`).

| Status | `*_at` | `*_error` |
|--------|--------|-----------|
| `NULL` | NULL | NULL |
| `pending` | NULL | NULL |
| `done` | NOT NULL | NULL |
| `skipped` | NOT NULL | NULL |
| `failed` | NULL | NOT NULL (non-empty) |

### Columns (12)

For each stage in `{classify, topics, billing, datalake}`:

| Column | Type |
|--------|------|
| `close_{stage}_status` | `CharField(max_length=16)`, nullable, choices as above |
| `close_{stage}_at` | `DateTimeField`, nullable |
| `close_{stage}_error` | `TextField`, nullable |

### Lifecycle shapes

**Shape A — Open, not claimed**

- `resolution = '2'`
- All four statuses `NULL`; all `*_at` / `*_error` NULL

**Shape B — Classify claimed or classify failed (still open)**

- `resolution = '2'`
- `close_classify_status ∈ {pending, failed}` with matching at/error shape
- topics/billing/datalake statuses `NULL`

**Shape C — Classify finished (atomic commit)**

- `resolution ∈ {0,1,3,4}`
- `close_classify_status ∈ {done, skipped}` with `close_classify_at` set
- topics/billing/datalake each `pending` or `skipped` (NOT NULL), with matching at/error shape

**Shape D — Downstream progress**

- Classify remains finished
- Each of topics/billing/datalake independently `pending|done|skipped|failed` with shape rules
- Legal example: topics `failed`, billing `done`, datalake `pending`
- Illegal: billing `done` while classify not finished; terminal resolution with downstream still `NULL` for pipeline-managed rows

### CheckConstraints (normative)

Per stage (×4): done/skipped requires at and forbids error; failed requires error and forbids at; pending forbids at and error; null status forbids at and error.

Cross-field:

- If `resolution = '2'`: topics/billing/datalake must be NULL; classify must not be `done`/`skipped`
- If classify `done`/`skipped`: resolution ≠ `'2'`
- If resolution ≠ `'2'` (after cutover + backfill): classify finished and topics/billing/datalake NOT NULL
- If any of topics/billing/datalake NOT NULL: classify finished

### Backfill

Rows with `resolution ≠ '2'`: set all four statuses to `done`, all `*_at` to `COALESCE(end_date, created_at, now())`, all errors NULL (*legacy assumed complete*).

Rows with `resolution = '2'`: leave all twelve columns NULL.

### Indexes

Partial/composite indexes supporting drain queries on each stage status in `{pending, failed}` (optionally including `project_id`).

## State machine

Module: `conversation_ms/close_daily/state_machine.py`

Workers NEVER assign stage columns directly.

Per-axis graph:

```text
NULL → pending → done
           ├→ skipped
           └→ failed → pending  (retry)
```

Key methods: `claim_classify`, `fail_classify`, `commit_classify_success` (atomic Shape B→C), reclaim/mark done|skipped|failed for topics/billing/datalake.

## Relationships

- `ConversationClassification` remains the topics **business** artifact; it does not replace `close_topics_status`.
- No new Batch/Record tables required for v1 (unlike archive); stages live on `Conversation`.
