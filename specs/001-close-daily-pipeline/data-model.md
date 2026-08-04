# Data Model: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md) | **Date**: 2026-07-28

## Entity: Conversation (unchanged schema for pipeline)

Table: `intelligences_conversation` (`conversation_ms.models.Conversation`).

`resolution` remains the **only** business close outcome on this model (`0`–`4`). **No** close-pipeline columns are added to `Conversation`.

Pipeline participation:

- **In / was in pipeline**: a `ClosePipelineRecord` row exists for the conversation
- **Never claimed / out-of-band terminal (Shape E)**: no `ClosePipelineRecord` (even if `resolution` is terminal)

## Entity: ClosePipelineRecord (1:1 control plane)

Table: e.g. `conversation_ms_closepipelinerecord`.

**One row per conversation that entered close-daily** (OneToOne → `Conversation`, CASCADE).

This is the durable control plane for classify / topics / billing / datalake. Keeps `Conversation` as a business entity.

### Stage vocabulary

Stored strings: `pending`, `done`, `skipped`, `failed`, `dead`. Absence of a stage = SQL `NULL` on that stage’s columns (never the string `"null"`).

| Status | `{stage}_at` (completed) | `{stage}_pending_at` | `{stage}_error` |
|--------|--------------------------|----------------------|-----------------|
| `NULL` | NULL | NULL | NULL |
| `pending` | NULL | NOT NULL | NULL |
| `done` | NOT NULL | NULL | NULL |
| `skipped` | NOT NULL | NULL | NULL |
| `failed` | NULL | NULL | NOT NULL (non-empty) |
| `dead` | NULL | NULL | NOT NULL (non-empty) |

### Columns

`conversation` — `OneToOneField(Conversation, on_delete=CASCADE, related_name="close_pipeline")` (unique; recommended as primary key or with unique constraint).

For each stage in `{classify, topics, billing, datalake}`:

| Column | Type |
|--------|------|
| `{stage}_status` | `CharField(max_length=16)`, nullable, choices as above |
| `{stage}_at` | `DateTimeField`, nullable — set when status becomes `done` or `skipped` |
| `{stage}_pending_at` | `DateTimeField`, nullable — set when entering `pending`; cleared on leave; **drain stale clock** |
| `{stage}_error` | `TextField`, nullable — required non-empty when status is `failed` or `dead` |
| `{stage}_reclaim_count` | `PositiveIntegerField`, default **0** — incremented on each automatic drain reclaim / stale re-enqueue |

**Datalake event progress** (in addition to the datalake quintuple):

| Column | Type | Role |
|--------|------|------|
| `datalake_classification_at` | `DateTimeField`, nullable | Set **once** when `conversation_classification` publish succeeds and outbox is marked published; never cleared on `failed`/`dead` |
| `datalake_topics_at` | `DateTimeField`, nullable | Set **once** when `topics` publish succeeds and outbox is marked published; never cleared on `failed`/`dead` |

Optional bookkeeping: `created_at`, `updated_at` on the record (not stage clocks).

Total stage fields on `ClosePipelineRecord`: **22** (status + at + pending_at + error + reclaim_count × 4 + 2 event ats).

Unlike billing, datalake has **no** consumer-side upsert safety net — intentional duplicate production is forbidden. Billing still uses producer discipline (`done` ⇒ no-op); Billing upsert is residual only (research R11). Outbox residual: R9 / R12. Dead letter: research R21.

### Entity: CloseDatalakeOutbox

Table: e.g. `conversation_ms_closedatalakeoutbox`.

Minimal schema (normative):

| Column | Type | Role |
|--------|------|------|
| `id` | PK | Surrogate |
| `conversation_id` | FK → `Conversation.uuid`, CASCADE | Parent conversation |
| `event_kind` | `CharField` | `classification` \| `topics` |
| `created_at` | `DateTimeField` auto | Intent created |
| `published_at` | `DateTimeField`, nullable | Set when external publish is considered successful |
| `last_error` | `TextField`, nullable | Last publish failure detail for this event kind (secondary to stage `*_error`) |

**Constraints:** `UNIQUE (conversation_id, event_kind)`.

Not a product stage — producer machinery only. Cleanup/archival deferred post-v1.

### Datalake worker algorithm (normative)

Operates on the conversation’s `ClosePipelineRecord` (+ outbox).

1. If `datalake_status` ∈ `{done, skipped}` → no-op.
2. **Classification event** (precondition: classify finished — always true once record is past Shape C init): if `datalake_classification_at` is NULL → ensure unique outbox `(conversation_id, classification)`; if `published_at` IS NULL → publish; on success set `published_at`, set `datalake_classification_at`, clear outbox `last_error` after publish OK.
3. **Topics event**:
   - If `topics_status` ∉ `{done, skipped}` → **do not** create/publish topics outbox; leave datalake `pending`.
   - If topics ∈ `{done, skipped}` and `datalake_topics_at` is NULL → unique outbox `(conversation_id, topics)`; publish via `build_topics_event` (bias path when skipped / no classification / no active topics); on success set `published_at` + `datalake_topics_at`.
   - **Forbidden:** set `datalake_topics_at` without a successful external publish.
4. Never insert a second outbox row for the same `(conversation_id, event_kind)` (DB UNIQUE).
5. When both event timestamps are set → `datalake_status = done`, set `datalake_at`, clear `datalake_pending_at`.
6. On failure of an attempted publish → `datalake_status = failed` with error; clear `datalake_pending_at`; **keep** event timestamps / published outbox rows; set outbox `last_error`.
7. Reclaim `failed → pending` clears stage **error** only, sets fresh `pending_at`; does **not** clear event timestamps or published outbox rows.

**Residual crash window (accepted):** publish succeeded then crash before mark → retry may republish once. UNIQUE prevents a second intent row.

**Enqueue graph (normative):**

```text
classify success ──► enqueue topics
                  ├──► enqueue billing
                  └──► enqueue datalake   # may send classification only

topics done/skipped ──► enqueue datalake  # publish topics event (incl. bias if skipped)
```

### Lifecycle shapes

Shapes refer to `(Conversation.resolution, ClosePipelineRecord?)`.

**Shape A — Open, not claimed**

- `resolution = '2'`
- **No** `ClosePipelineRecord`

**Shape B — Classify claimed, failed, or dead (still open)**

- `resolution = '2'`
- `ClosePipelineRecord` exists
- `classify_status ∈ {pending, failed, dead}` with matching at/pending_at/error shape
- topics/billing/datalake statuses `NULL`; datalake event ats NULL
- **`classify_status = dead`**: intentional poison stop — conversation stays In Progress with **no** automatic path to a terminal resolution. Ops reclaim `dead → pending` after fixing the cause, or an out-of-band resolution update (→ Shape E). Do **not** auto-set Unclassified from classify `dead`.

**Shape C — Classify finished (atomic commit via state machine)**

- `resolution ∈ {0,1,3,4}`
- `ClosePipelineRecord` exists
- `classify_status ∈ {done, skipped}` with `classify_at` set and `classify_pending_at` NULL
- topics and billing each `pending` or `skipped` (NOT NULL) — billing `skipped` only for business-ineligible payload
- **datalake always `pending`** at Shape C in v1; both event ats NULL; `datalake_pending_at` set

**Shape D — Downstream progress**

- Classify remains finished; record exists
- Each of topics/billing/datalake independently `pending|done|skipped|failed|dead` with shape rules
- Legal datalake partial: `datalake_classification_at` set, `datalake_topics_at` NULL while topics still open or failed
- Illegal: publish topics datalake event while `topics_status` ∉ `{done, skipped}`
- Illegal: set `datalake_topics_at` without successful publish
- `dead` is terminal for automatic drain (ops may reopen)

**Shape E — Terminal without pipeline (out-of-band)**

- `resolution ∈ {0,1,3,4}`
- **No** `ClosePipelineRecord`
- Allowed so admin/hotfix resolution updates and Phase 1→2 gap closes do not require a pipeline row
- Drain **MUST NOT** invent work for Shape E
- Production close-daily **MUST NOT** leave Shape E after classify on the new path; it MUST `commit_classify_success` (Shape C)

### CheckConstraints (normative — on `ClosePipelineRecord`)

Django `CheckConstraint`s on the pipeline table only (static, always on). Cross-table rules involving `Conversation.resolution` are **application / state-machine** (+ tests), not DB FKs spanning resolution.

**Per stage classify / topics / billing / datalake:**

- `done`/`skipped` ⇒ `{stage}_at` NOT NULL and `{stage}_error` IS NULL and `{stage}_pending_at` IS NULL
- `failed` ⇒ `{stage}_error` NOT NULL and `{stage}_at` IS NULL and `{stage}_pending_at` IS NULL
- `dead` ⇒ `{stage}_error` NOT NULL and `{stage}_at` IS NULL and `{stage}_pending_at` IS NULL
- `pending` ⇒ `{stage}_at` IS NULL and `{stage}_error` IS NULL and `{stage}_pending_at` NOT NULL
- status IS NULL ⇒ `{stage}_at`, `{stage}_pending_at`, and `{stage}_error` all NULL
- `{stage}_reclaim_count` ≥ 0 always (DB default 0)

**Datalake event ats (additional):**

- If all datalake stage fields NULL (status/at/pending_at/error) ⇒ both event ats NULL
  (When record is Shape B, datalake columns are all NULL.)
- `datalake_status = pending` ⇒ `datalake_at` IS NULL; event ats may be 0 or 1 set. Both event ats NOT NULL while pending is illegal — promote to `done` in the same transaction that sets the second at
- Constraint: both event ats NOT NULL ⇒ `datalake_status` IN (`done`, `skipped`) AND `datalake_at` NOT NULL
- `done`/`skipped` ⇒ `datalake_at` NOT NULL, error NULL, pending_at NULL, **both** event ats NOT NULL
- `failed` ⇒ error NOT NULL, `datalake_at` IS NULL, pending_at NULL; zero or one event at may be set
- `dead` ⇒ same shape as `failed` for at/pending_at/error; reclaim_count already at/above budget

**Cross-field on the record (always):**

1. If any of topics/billing/datalake status IS NOT NULL: `classify_status` IN (`done`, `skipped`)
2. If `classify_status` IN (`done`, `skipped`): topics/billing/datalake statuses are NOT NULL (Shape C invariant for rows that finished classify on close-daily)

**Application-only (tests + state machine; involve `Conversation.resolution`):**

- Shape B ⇒ `resolution = '2'`
- Shape C/D ⇒ `resolution <> '2'`
- close-daily MUST NOT create Shape E; drain ignores missing records

### Concurrency / claim semantics

- **`claim_classify`**: under `select_for_update` on the conversation row (and/or unique insert of `ClosePipelineRecord`): succeed only from Shape A → insert record with `classify_status=pending`, `classify_pending_at=now()`. Concurrent claims: unique OneToOne — one wins, the other no-ops.
- **Downstream workers**: `select_for_update` on `ClosePipelineRecord` before `pending` → `done`/`skipped`/`failed`; if already terminal, no-op. Leaving `pending` clears `{stage}_pending_at`.
- **Attempt heartbeat**: at each worker attempt start (incl. Celery autoretry), while still `pending`, refresh `{stage}_pending_at = now()` under row lock.
- **Duplicate enqueues**: acceptable; workers no-op safely.
- **Lambda single-flight** (FR-007): Celery queue `close_lambda` concurrency 1 (classify + topics).
- **Selector**: existing per-project Redis lock for dispatch/claim batching.

### Celery retry policy (normative)

| Concern | Rule |
|---------|------|
| Status while retrying | Remains `pending` on `ClosePipelineRecord` |
| Who marks `failed` | The **stage task** (or failure handler), not drain |
| When to mark `failed` | (1) Celery `max_retries` exhausted on retryable exception, or (2) non-retryable error (e.g. empty Billing queue URL) |
| SIGKILL / hard death | Stays `pending` → **drain stale `pending_at`** |
| Soft time limit | Prefer mark `failed` with timeout when catchable; else same as SIGKILL |
| `pending_at` across retries | **Refreshed** each attempt (heartbeat) |
| Defaults | Classify/topics `max_retries` **3**; billing/datalake `max_retries` **5**; `CLOSE_PIPELINE_STALE_PENDING_SECONDS` **1800** (≥ retry window) |

### Drain eligibility (normative)

Settings: `CLOSE_PIPELINE_STALE_PENDING_SECONDS` (**1800**), `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` (**100**), `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` (**5**), Beat every **10 minutes**.

Select from **`ClosePipelineRecord`** (inner join `Conversation` as needed for project filters):

| Case | Eligible when |
|------|----------------|
| `failed` (budget remaining) | `{stage}_status = failed` AND `{stage}_reclaim_count < MAX` → increment reclaim count, reclaim to `pending` + fresh `pending_at`, enqueue |
| stale `pending` (budget remaining) | `{stage}_pending_at < now() - TTL` **and** stage-specific preconditions hold **and** reclaim_count < MAX → increment reclaim count, re-enqueue (keep `pending`, refresh `pending_at`) |
| budget exhausted | `{stage}_status ∈ {failed}` OR stale `pending` eligible by age/preconditions, **and** `{stage}_reclaim_count >= MAX` → mark `dead` with error; **do not** enqueue |
| `dead` | **Never** selected for automatic reclaim |

Cap at `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` per stage per run (oldest `pending_at` / failed first).

**Datalake special rule:** do **not** treat datalake as stale-pending solely by age while `topics_status ∉ {done, skipped}`. Reclaim/requeue when:

- `datalake_status = failed`, or
- datalake `pending` and stale **and** topics ∈ `{done, skipped}`, or
- datalake `pending` and stale **and** `datalake_classification_at` IS NULL

(subject to reclaim budget / `dead` rules above)

Drain MUST NOT automatically reclaim `skipped` or `dead`. Drain MUST NOT create records for Shape E.

### Backfill

For each `Conversation` with `resolution ≠ '2'` and **no** `ClosePipelineRecord`: insert record with all four statuses `done`; set all `{stage}_at` and both datalake event ats to `COALESCE(end_date, created_at, now())`; all `{stage}_pending_at` NULL; all errors NULL; all `{stage}_reclaim_count = 0` (*legacy assumed complete*). No outbox rows required.

Conversations with `resolution = '2'`: leave without a pipeline record (Shape A).

### Indexes

On `ClosePipelineRecord`, partial/composite indexes supporting drain per stage where status ∈ `{pending, failed}`, including `{stage}_pending_at` and optionally `{stage}_reclaim_count`. Join path to `Conversation.project_id` as needed for project-scoped ops. Index or filter for ops queries on `{stage}_status = dead`.

## State machine

Module: `conversation_ms/close_daily/state_machine.py`

Workers NEVER assign pipeline columns directly; only via `ClosePipelineStateMachine` mutating `ClosePipelineRecord` (+ `Conversation.resolution` on classify commit).

Per-axis graph:

```text
NULL → pending → done
           ├→ skipped ⇄ pending   (ops reclaim only; not automatic drain)
           ├→ failed  → pending   (drain / ops reclaim; increments reclaim_count)
           └→ failed|stale → dead (drain when reclaim_count would exceed MAX)
dead → pending                 (ops only; resets reclaim_count to 0)
```

Entering `pending` sets `{stage}_pending_at = now()`. Leaving `pending` clears it.

**Billing init at Shape C:**

- Business-ineligible payload → may init as `skipped`
- Otherwise → `pending` (worker marks `failed` if queue URL / transport is bad)
- Never init billing as `skipped` solely because queue URL is empty

**Retry ownership:**

- Celery autoretry while `pending`; see Celery retry policy
- `failed → pending`: drain (automatic, budget permitting) or ops
- Exhausted budget → `dead` (automatic drain); no enqueue
- `skipped → pending` / `dead → pending`: ops-only; drain never selects `skipped` or `dead`

Key methods: `claim_classify` (insert record), `fail_classify`, `commit_classify_success` (atomic Shape B→C on conversation + record; caller enqueues topics + billing + datalake), mark done|skipped|failed|dead for topics/billing/datalake, reclaim for retry (incl. ops `dead→pending`).

## Relationships

- `Conversation` — business entity; `resolution` only.
- `ClosePipelineRecord` — 1:1 close control plane (**22** stage fields).
- `ConversationClassification` — topics business artifact; does not replace `topics_status`.
- `CloseDatalakeOutbox` — unique intent for datalake production; not a product stage.
- Rejected for v1: 18/22 columns on `Conversation`; one Django model per stage; fully normalized stage-row table (may revisit post-v1 for extensibility); SQS/Celery DLQ as substitute for status `dead`.
