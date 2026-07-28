# Data Model: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md) | **Date**: 2026-07-28

## Entity: Conversation (extended)

Table: `intelligences_conversation` (`conversation_ms.models.Conversation`).

`resolution` remains the business outcome (`0`–`4`). Pipeline fields are control plane only.

### Stage vocabulary

Stored strings: `pending`, `done`, `skipped`, `failed`. Absence = SQL `NULL` (never the string `"null"`).

| Status | `*_at` (completed) | `*_pending_at` | `*_error` |
|--------|--------------------|----------------|-----------|
| `NULL` | NULL | NULL | NULL |
| `pending` | NULL | NOT NULL | NULL |
| `done` | NOT NULL | NULL | NULL |
| `skipped` | NOT NULL | NULL | NULL |
| `failed` | NULL | NULL | NOT NULL (non-empty) |

### Columns

For each stage in `{classify, topics, billing, datalake}`:

| Column | Type |
|--------|------|
| `close_{stage}_status` | `CharField(max_length=16)`, nullable, choices as above |
| `close_{stage}_at` | `DateTimeField`, nullable — set when status becomes `done` or `skipped` |
| `close_{stage}_pending_at` | `DateTimeField`, nullable — set when entering `pending`; cleared on leave; **drain stale clock** |
| `close_{stage}_error` | `TextField`, nullable |

**Datalake event progress** (in addition to the datalake quadruple above):

Unlike billing, datalake has **no** consumer-side upsert safety net — intentional duplicate production is forbidden. Billing still uses the same *producer* discipline (`done` ⇒ no-op); Billing upsert is only a residual safety net, not the recovery strategy (see research R11). Outbox residual window: see research R9 / R12.

| Column | Type | Role |
|--------|------|------|
| `close_datalake_classification_at` | `DateTimeField`, nullable | Set **once** when `conversation_classification` event publish succeeds and outbox is marked published; never cleared on `failed` |
| `close_datalake_topics_at` | `DateTimeField`, nullable | Set **once** when `topics` event publish succeeds and outbox is marked published; never cleared on `failed` |

Total on `Conversation`: **18 columns** (status + at + pending_at + error × 4 stages = 16, plus 2 datalake event ats).

No separate `pipeline_managed` flag. Pipeline participation is inferred:

- **In / was in pipeline**: `close_classify_status IS NOT NULL`
- **Never claimed / out-of-band terminal**: all stage statuses `NULL` (even if `resolution` is terminal)

### Entity: CloseDatalakeOutbox

Table: e.g. `conversation_ms_closedatalakeoutbox` (name per Django defaults).

Minimal schema (normative):

| Column | Type | Role |
|--------|------|------|
| `id` | PK | Surrogate |
| `conversation_id` | FK → `Conversation.uuid`, CASCADE | Parent conversation |
| `event_kind` | `CharField` | `classification` \| `topics` |
| `created_at` | `DateTimeField` auto | Intent created |
| `published_at` | `DateTimeField`, nullable | Set when external publish is considered successful |
| `last_error` | `TextField`, nullable | Last publish failure detail for this event kind (secondary to stage `*_error`; useful when one of two events failed) |

**Constraints:** `UNIQUE (conversation_id, event_kind)`.

No fifth product stage — outbox is producer machinery only.

### Datalake worker algorithm (normative)

1. If status ∈ `{done, skipped}` → no-op.
2. **Classification event** (precondition: classify finished — always true when datalake is non-NULL): if `close_datalake_classification_at` is NULL → ensure unique outbox row `(conversation_id, classification)`; if `published_at` IS NULL → publish; on success set `published_at`, set `close_datalake_classification_at`, clear outbox `last_error` in the same DB transaction after publish returns OK.
3. **Topics event**:
   - If topics status ∉ `{done, skipped}` → **do not** create/publish topics outbox; leave datalake `pending` (classification may already be sent).
   - If topics ∈ `{done, skipped}` and `close_datalake_topics_at` is NULL → unique outbox `(conversation_id, topics)`; publish using `build_topics_event` (for `skipped` / no classification / no active topics: existing **bias** payload); on success set `published_at` + `close_datalake_topics_at`.
   - **Forbidden:** set `close_datalake_topics_at` without a successful external publish.
4. Never insert a second outbox row for the same `(conversation_id, event_kind)` (DB UNIQUE).
5. When both event timestamps are set → transition to `done`, set `close_datalake_at`, clear `close_datalake_pending_at`.
6. On failure of an attempted publish → `failed` with error; clear `pending_at`; **keep** any event timestamps / published outbox rows already recorded; set outbox `last_error` for the attempted kind.
7. Reclaim `failed → pending` clears stage **error** only, sets fresh `pending_at`; does **not** clear event timestamps or published outbox rows.

**Residual crash window (accepted):** if publish to datalake succeeds and the process dies before `published_at` / event-at persist, retry may publish again. UNIQUE prevents a second intent row; it does not eliminate that residual. Same class of residual as Billing SQS accept → mark done.

**Enqueue graph (normative):**

```text
classify success ──► enqueue topics
                  ├──► enqueue billing
                  └──► enqueue datalake   # may send classification only

topics done/skipped ──► enqueue datalake  # publish topics event (incl. bias if skipped)
```

### Lifecycle shapes

**Shape A — Open, not claimed**

- `resolution = '2'`
- All stage statuses `NULL`; all timestamps/errors/pending_ats NULL (including both datalake event ats)

**Shape B — Classify claimed or classify failed (still open)**

- `resolution = '2'`
- `close_classify_status ∈ {pending, failed}` with matching at/pending_at/error shape
- topics/billing/datalake statuses `NULL`; datalake event ats NULL

**Shape C — Classify finished (atomic commit via state machine)**

- `resolution ∈ {0,1,3,4}`
- `close_classify_status ∈ {done, skipped}` with `close_classify_at` set and `pending_at` NULL
- topics and billing each `pending` or `skipped` (NOT NULL), with matching shapes — billing `skipped` only for business-ineligible payload, never for empty queue URL
- **datalake always `pending`** at Shape C in v1 (never initialized as `skipped`); both event ats NULL; `close_datalake_pending_at` set

**Shape D — Downstream progress**

- Classify remains finished
- Each of topics/billing/datalake independently `pending|done|skipped|failed` with shape rules
- Legal datalake partial after classify, topics still open: status `pending`, `close_datalake_classification_at` set, `close_datalake_topics_at` NULL
- Legal datalake partial after topics failure: classification may be sent; topics event waits until topics is reclaimed and finishes `done`/`skipped`
- Illegal: publish topics datalake event while topics status ∉ `{done, skipped}`
- Illegal: set `close_datalake_topics_at` without a successful publish
- Illegal: re-enqueue path that ignores an already-set event timestamp / published outbox

**Shape E — Terminal without pipeline (out-of-band)**

- `resolution ∈ {0,1,3,4}`
- All stage statuses `NULL` (datalake event ats NULL)
- Allowed in the **database** so CheckConstraints stay static and do not break admin/hotfix paths that set `resolution` outside close-daily
- Drain **MUST NOT** select Shape E
- Production close-daily **MUST NOT** create Shape E; it MUST use `commit_classify_success` (Shape C)

### CheckConstraints (normative — static, always on)

Django `CheckConstraint`s cannot be phased by “after cutover”. Only encode invariants that are true for every row at all times.

**Per stage classify / topics / billing / datalake (status/at/pending_at/error):**

- `done`/`skipped` ⇒ `*_at` NOT NULL and `*_error` IS NULL and `*_pending_at` IS NULL
- `failed` ⇒ `*_error` NOT NULL and `*_at` IS NULL and `*_pending_at` IS NULL
- `pending` ⇒ `*_at` IS NULL and `*_error` IS NULL and `*_pending_at` NOT NULL
- status IS NULL ⇒ `*_at`, `*_pending_at`, and `*_error` all NULL

**Datalake event ats (additional):**

- status IS NULL ⇒ both event ats NULL (and the quadruple NULL as above)
- `pending` ⇒ `close_datalake_at` IS NULL; event ats may be 0 or 1 set. Both event ats NOT NULL while `pending` is illegal — application MUST promote to `done` in the same transaction that sets the second at
- Constraint: both event ats NOT NULL ⇒ status IN (`done`, `skipped`) AND `close_datalake_at` NOT NULL
- `done`/`skipped` ⇒ `close_datalake_at` NOT NULL, error NULL, pending_at NULL, **both** event ats NOT NULL
- `failed` ⇒ error NOT NULL, `close_datalake_at` IS NULL, pending_at NULL; zero or one event at may be set

**Cross-field (always):**

1. If `resolution = '2'`: topics/billing/datalake statuses IS NULL; classify status NOT IN (`done`, `skipped`)
2. If classify status IN (`done`, `skipped`): `resolution <> '2'`
3. If any of topics/billing/datalake status IS NOT NULL: classify status IN (`done`, `skipped`)

**Explicitly NOT a DB constraint:**

- `resolution <> '2'` ⇒ stages filled

That rule is **application-only** for the close-daily path (Shape C via state machine). Backfill still sets legacy terminals to all-`done` so drain does not treat history as incomplete. Shape E remains legal for rare out-of-band closes.

### Concurrency / claim semantics

- **`claim_classify`**: `SELECT … FOR UPDATE` on the conversation row; succeed only from Shape A (`resolution='2'`, classify status NULL) → Shape B `pending` with `close_classify_pending_at = now()`. Concurrent claims: one wins, the other no-ops or skips enqueue.
- **Downstream workers**: `select_for_update` before transitioning `pending` → `done`/`skipped`/`failed`; if status already terminal, no-op (idempotent). Leaving `pending` clears `pending_at`.
- **Attempt heartbeat**: at the start of each worker attempt (including Celery autoretry), while status is still `pending`, refresh `pending_at = now()` under `select_for_update`. This prevents drain from treating in-flight Celery retries as stale.
- **Duplicate enqueues**: concurrent Celery tasks for the same conversation+stage are possible (classify finish + drain, or double enqueue). Workers MUST no-op safely; contention is acceptable inefficiency, not a correctness bug.
- **Lambda single-flight** (FR-007): separate from per-row claim — enforced by Celery queue `close_lambda` with **worker concurrency = 1** (classify + topics). Not a Redis lock per conversation for Lambda; serialization is queue-wide.
- **Selector**: keep existing per-project Redis lock for dispatch/claim batching (does not replace row-level claim).

### Celery retry policy (normative)

Applies to all four stage tasks unless a setting overrides per stage.

| Concern | Rule |
|---------|------|
| Status while retrying | Remains `pending` |
| Who marks `failed` | The **stage task** (or its failure handler), not drain |
| When to mark `failed` | (1) Celery `max_retries` exhausted on a retryable exception, or (2) non-retryable business/config error on first handling (e.g. empty Billing queue URL after worker starts — may fail immediately without burning all retries) |
| SIGKILL / hard worker death | Status stays `pending`; no handler runs → **drain stale `pending_at`** recovers |
| Soft time limit | Prefer catch soft limit, mark `failed` with timeout error when possible; if hard-killed, same as SIGKILL |
| `pending_at` across Celery retries | **Refreshed** at each attempt start (heartbeat); not left frozen from first claim |
| Defaults (settings) | `CLOSE_PIPELINE_CELERY_MAX_RETRIES` default **5**; exponential backoff via Celery (`CLOSE_PIPELINE_CELERY_RETRY_BACKOFF` default **true**). `CLOSE_PIPELINE_STALE_PENDING_SECONDS` MUST exceed the worst-case Celery retry window (default suggestion **3600**) so drain and Celery do not race while retries are healthy |

### Drain eligibility (normative)

Settings: `CLOSE_PIPELINE_STALE_PENDING_SECONDS`, `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` (default **100** per stage per beat tick).

Select stages where `close_classify_status IS NOT NULL` (exclude Shape E) and:

| Case | Eligible when |
|------|----------------|
| `failed` | stage `failed` (reclaim → `pending` + fresh `pending_at`, then enqueue) |
| stale `pending` | `pending_at < now() - TTL` **and** stage-specific preconditions hold |

Each drain run MUST cap selection at `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` per stage (ordered by oldest `pending_at` / `failed` first).

**Datalake special rule:** do **not** treat datalake as stale-pending solely by age while `close_topics_status ∉ {done, skipped}`. That row may be legitimately waiting for the topics event precondition (classification may already be sent). Re-enqueue/reclaim datalake when:

- status `failed`, or
- status `pending` and stale **and** topics ∈ `{done, skipped}` (topics event still missing), or
- status `pending` and stale **and** `close_datalake_classification_at` IS NULL (classification still outstanding — age wait is meaningful)

Harmless no-op requeues of “waiting on topics” MUST be avoided by the rule above.

Drain MUST NOT automatically reclaim `skipped`.

### Backfill

Rows with `resolution ≠ '2'`: set all four statuses to `done`; set all `*_at` and both datalake event ats to `COALESCE(end_date, created_at, now())`; all `*_pending_at` NULL; all errors NULL (*legacy assumed complete*). No outbox rows required for legacy (drain must not replay; workers no-op on `done`).

Rows with `resolution = '2'`: leave all pipeline columns NULL.

### Indexes

Partial/composite indexes supporting drain queries on each stage where status ∈ `{pending, failed}` (optionally including `project_id`), including `close_{stage}_pending_at` for stale filters. Drain filters MUST require `close_classify_status IS NOT NULL` (exclude Shape E).

## State machine

Module: `conversation_ms/close_daily/state_machine.py`

Workers NEVER assign stage columns directly.

Per-axis graph:

```text
NULL → pending → done
           ├→ skipped ⇄ pending   (ops reclaim only; not automatic drain)
           └→ failed  → pending   (drain / ops reclaim)
```

Entering `pending` always sets `pending_at = now()` (including reclaim and attempt heartbeat). Leaving `pending` always clears `pending_at`.

**Billing init at Shape C (normative):**

- Business-ineligible payload → may init as `skipped`
- Otherwise → init as `pending` (even if queue URL looks wrong at commit time — worker confirms and marks `failed` if config/infra is bad)
- Never init billing as `skipped` solely because queue URL is empty

**Retry ownership:**

- Celery may autoretry while the stage remains `pending` (before marking `failed`); see Celery retry policy
- `failed → pending` is performed by **drain** (automatic) or ops; manual reclaim allowed
- `skipped → pending` is **ops-only** (state-machine method); drain never selects `skipped`

Key methods: `claim_classify`, `fail_classify`, `commit_classify_success` (atomic Shape B→C; caller enqueues topics + billing + datalake), mark done|skipped|failed for topics (caller re-enqueues datalake on topics `done`/`skipped`), reclaim (`failed→pending`, and ops `skipped→pending`) / mark for billing/datalake.

## Relationships

- `ConversationClassification` remains the topics **business** artifact; it does not replace `close_topics_status`.
- `CloseDatalakeOutbox` supports durable unique intent for datalake event production; not a product stage. **Cleanup/archival deferred post-v1** (~2 rows per conversation retained).
- No archive-style Batch/Record tables required for classify/topics/billing; those stages live on `Conversation`.
