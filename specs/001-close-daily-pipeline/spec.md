# Feature Specification: Close-Daily Four-Stage Pipeline

**Feature Branch**: `001-close-daily-pipeline`

**Created**: 2026-07-28

**Status**: Draft

**Spec version**: 1.5.4

**Related artifacts**: [plan.md](./plan.md), [tasks.md](./tasks.md), [research.md](./research.md), [data-model.md](./data-model.md), [quickstart.md](./quickstart.md)

**Scope**: **nexus-conversations backend only** (no frontend, no Billing service changes, no reconcile against Billing totals).

**Input**: Restructure the daily conversation close flow so classify (resolution), topics, billing, and datalake are separately tracked stages with durable status, timestamps, and errors; illegal combinations are unrepresentable; Lambda work is single-flight; failures can resume without losing later stages.

## Context and motivation

The close-daily project Celery task currently runs resolution classification, topics classification, Billing SQS publish, and datalake event enqueue inside one long-lived worker (often with parallel classify threads). When the worker hits hard time limit (`SIGKILL`), conversations may already have a terminal `resolution` and will not be selected again—billing/datalake/topics can stop mid-flight with **no durable stage marker**.

Operators and engineers need to answer: **where did this conversation stop?** Stage tracking and per-stage workers exist so edge cases are handled deliberately (resume, skip, fail) — **not** by blindly re-running side effects. Delay from serial processing is acceptable. Cross-checking counts with Billing is **out of scope**.

Billing is **prepared** to upsert if it ever receives the same close twice; that is a **safety net**, not a license for Conversations to produce duplicates. The pipeline MUST still treat billing send as a once-per-conversation success path (`done` ⇒ no-op on retry).

Design principle: **make unreasonable states invalid** (same spirit as conversations S3 archive tracking): application state machine + database constraints so illegal status/timestamp/error/resolution combinations cannot persist.

## Glossary

| Term | Meaning |
|------|---------|
| **Resolution** | Business outcome on `Conversation.resolution` (`0` Resolved, `1` Unresolved, `2` In Progress, `3` Unclassified, `4` Has Chat Room) |
| **ClosePipelineRecord** | 1:1 control-plane row for a conversation in the close pipeline (stage status/at/pending_at/error + datalake event ats). Not columns on `Conversation` |
| **Close pipeline stage** | One of: classify, topics, billing, datalake |
| **Stage status** | `NULL` (stage not started on the record) \| `pending` \| `done` \| `skipped` \| `failed` \| `dead` |
| **Stage completed-at** | Timestamp set only when status is `done` or `skipped` (`{stage}_at` on `ClosePipelineRecord`) |
| **Stage pending-at** | Timestamp set only while status is `pending` (`{stage}_pending_at`); drain stale clock |
| **Stage error** | Non-empty text set when status is `failed` or `dead` |
| **Dead letter (logical)** | Stage status `dead`: automatic drain **stops** reclaiming after reclaim budget for **poison**. Not an SQS/Celery DLQ. Ops may reopen `dead → pending` (single or bulk) via state machine only |
| **Stage reclaim count** | `{stage}_reclaim_count` on `ClosePipelineRecord`; incremented on budget-consuming drain reclaim; drives transition to `dead` (see outage policy) |
| **Poison vs brownout** | **Poison** = per-conversation / bad payload failure. **Brownout/outage** = shared infra (Billing SQS, broker, misconfig affecting many rows). Same `dead` status; different drain budget rules |
| **Billing outage mode (v1)** | Manual **pause** (`CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE`): while on, drain may re-enqueue billing but MUST NOT consume reclaim budget toward `dead`. Automatic rate/Redis circuit is **deferred post-v1** |
| **Classify** | Resolution path (resolution Lambda or chat-room / no-messages short-circuit) that produces terminal resolution |
| **Topics** | Separate topics classifier Lambda + `ConversationClassification` persistence |
| **Billing stage** | Publish conversation-close message to Billing SQS |
| **Datalake stage** | Orchestrates **two** events with different preconditions: `conversation_classification` after classify; `topics` after topics finishes (`done` **or** `skipped`). No intentional duplicates; each event has its own durable sent timestamp |
| **Legacy assumed complete** | Backfill for conversations already terminal before this feature: create `ClosePipelineRecord` with all four stages `done` so drain does not replay history (not a verified send audit) |
| **Drain** | Periodic job that requeues `failed` or stale `pending` stages on existing `ClosePipelineRecord`s with valid preconditions |

## Program phases

| Phase | Backend deliverable |
|-------|---------------------|
| **1 — Foundation** | `ClosePipelineRecord` (stage fields incl. `dead` + reclaim counts) + `CloseDatalakeOutbox`, CheckConstraints, backfill, `ClosePipelineStateMachine`, tests; close runtime unchanged |
| **2 — Cutover** | Split classification APIs; four Celery stage workers; selector claim+enqueue; serial `close_lambda` queue; remove inline ThreadPool path |
| **3 — Drain & harden** | Drain beat, stale reclaim, **dead-letter**, **billing pause** (v1 outage), metrics; automatic billing circuit → follow-up |

## Clarifications (locked)

### Session 2026-07-28

- Q: Separate Celery stages for classify vs topics? → **A:** Yes — two Lambdas, two stages, equal tracking fidelity.
- Q: Track only billing/datalake? → **A:** No — all four stages.
- Q: Gate billing on topics success? → **A:** No — billing requires classify finished only; topics failure must not block Billing.
- Q: Dependency DAG for side effects? → **A:**
  ```text
  classify ──► billing
            └──► datalake event: conversation_classification
  topics ──────► datalake event: topics
  ```
  Topics failure must not block billing or the classification datalake event; it only blocks the topics datalake event.
- Q: Lambda parallelism? → **A:** Single-flight; classify + topics share concurrency-1 queue; remove ThreadPool fan-out on close path.
- Q: Billing cross-check / reconcile? → **A:** Out of scope for this feature.
- Q: Historical conversations? → **A:** Backfill terminal rows as all-four `done` (*legacy assumed complete*); not a real send audit.
- Q: Billing duplicates on retry? → **A:** Billing **can** upsert duplicates as a safety net, but Conversations **MUST NOT** design to rely on that. Tracking + idempotent stage workers exist so a successful billing send is recorded and not repeated. Accidental duplicate from a narrow crash window is mitigated by Billing — it is not an intended operating mode.
- Q: Datalake duplicates on retry? → **A:** **Not acceptable** as an operating mode — datalake does not dedupe. Use per-event tracking + unique outbox. A residual crash window (publish succeeded, mark published failed) may still produce one duplicate — accepted and documented (same class of residual as Billing); no sink idempotency key in v1.
- Q: Topics stage `skipped` → datalake topics event? → **A:** Still **publish** the topics event (parity with today: `build_topics_event` with `bias` / empty topic metadata when no classification or no active topics). Do **not** omit the event and do **not** set `close_datalake_topics_at` without publishing. Only `topics=failed` (or still `pending`) blocks the topics event.
- Q: Stale `pending` clock? → **A:** Dedicated `{stage}_pending_at` on `ClosePipelineRecord`; set on enter `pending`, cleared on leave. Drain uses `pending_at < now() - CLOSE_PIPELINE_STALE_PENDING_SECONDS`. Do **not** use Conversation `updated_at` or `{stage}_at` (null while pending).
- Q: Where do stage columns live? → **A:** On a separate **`ClosePipelineRecord`** (OneToOne → `Conversation`), not on `Conversation`. Rejected for v1: 18 cols on Conversation; one Django model per stage; fully normalized stage-row table.
- Q: Datalake initialized `skipped` at Shape C? → **A:** **No** in v1 — datalake always starts `pending` after classify success. Topics may init as `skipped`. Billing init rules below. Datalake `skipped` is not used on the close-daily path in v1.
- Q: Billing `skipped` vs `failed` at init? → **A:** **Business ineligible** (conversation cannot form a valid Billing payload) → init/mark `skipped` (intentional, no automatic retry). **Infra/config** (empty/missing Billing queue URL, publish transport errors after Celery retries) → `failed` so drain retries after the config is fixed. Never use `skipped` for “queue URL missing during deploy”.
- Q: `skipped → pending`? → **A:** Allowed as **ops reclaim only** (state-machine method) for topics/billing/datalake — not automatic drain. Classify `skipped` stays terminal unless an explicit ops path is added later.
- Q: When does `pending` become `failed`? → **A:** The stage worker marks `failed` when Celery `max_retries` is exhausted or the error is non-retryable. SIGKILL / hard kill leaves `pending` → drain stale path. See data-model “Celery retry policy”.
- Q: Phase 1→2 deploy gap / Shape E? → **A:** Preferred: ship foundation+cutover in the **same release train**. If Phase 1 lands alone, old runtime still sends billing/datalake — Shape E means **no `ClosePipelineRecord`** (tracking gap only), not lost Billing delivery. Gap backfill into the new pipeline is **out of scope** for v1.
- Q: Outbox table growth? → **A:** Cleanup/archival **deferred post-v1**; accepted unbounded growth at ~2 rows/conversation until then.
- Q: Extensibility? → **A:** New stage = add columns on `ClosePipelineRecord` + constraints + state-machine methods + worker + drain branch + enqueue edge (v1). Normalized stage rows may be revisited post-v1.

### Session 2026-08-04

- Q: Dead letter for poison / endless reclaim? → **A:** **Logical dead letter** — stage status `dead` on `ClosePipelineRecord` after automatic reclaim budget is exhausted. **Not** a new SQS/Celery DLQ. Drain MUST NOT reclaim `dead`. Ops-only `dead → pending` (resets reclaim count) via state machine.
- Q: What increments reclaim budget? → **A:** Each automatic drain action that reclaims `failed → pending` or re-enqueues stale `pending` increments `{stage}_reclaim_count`. When the next reclaim would exceed `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS`, drain marks `dead` (with error) instead of re-enqueueing.
- Q: Operational limits (Celery / stale / drain)? → **A:** Locked defaults (settings/env, overridable):
  | Setting | Default |
  |---------|---------|
  | Classify/topics Celery `max_retries` | **3** |
  | Billing/datalake Celery `max_retries` | **5** |
  | `CLOSE_PIPELINE_STALE_PENDING_SECONDS` | **1800** (30 min) |
  | Drain Beat interval | **10 min** |
  | `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` | **5** |
  | `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` | **100** (unchanged) |
  Stale TTL MUST be ≥ Celery retry window for that stage so healthy autoretries are not stealthed by drain.
- Q: Throughput / volume under concurrency-1? → **A:** Serial `close_lambda` **will** increase wall-clock classify/topics time vs today’s ThreadPool. Ops target: yesterday’s claimed cohort should reach classify finished (or classify `dead`/`failed` terminal for that attempt budget) within **12 hours** of the selector run that claimed it. Formula for capacity review: `eligible_conversations × p95_lambda_seconds / concurrency(1)`. If the target is missed in staging soak, options are (in order): measure and tune timeouts; discuss Lambda concurrency/batch with models; **not** silently raise Celery fan-out without Lambda capacity agreement.
- Q: Does dead letter change deploy topology? → **A:** No new Argo app. Same Conversations image; optional dedicated celery-worker Deployment for `close_lambda` concurrency 1 remains configuration, not a new product.
- Q: Classify reaches `dead` while still In Progress? → **A:** **Expected for poison classify.** Conversation remains `resolution = In Progress` indefinitely; automatic drain stops. Recovery is **ops-only via state machine** (Session 2026-08-07). MUST NOT auto-fallback to Unclassified solely because classify is `dead`.

### Session 2026-08-05

- Q: Mass `dead` when Billing/SQS is down (brownout)? → **A:** **Must not** turn a shared infra outage into cohort-wide `dead`. Ops MUST have a **bulk** `dead → pending` path. No SQS DLQ.
  - **v1 (ship):** `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` — while true, drain MAY re-enqueue billing but MUST **not** increment `billing_reclaim_count` and MUST **not** promote billing to `dead`.
  - **Post-v1 (design locked, deferred for complexity):** automatic rate/Redis circuit below — Session 2026-08-07 confirms deferral.
- Q: What is in/out of outage? → **A:** Billing (SQS / empty queue URL / transport). Classify/topics Lambda brownout mass-`dead` is **accepted v1 risk** (no second circuit in v1). Datalake transport circuit: **NOT** in v1 (accepted risk).
- Q: How is the **automatic** billing outage circuit calculated (locked for post-v1)? → **A:** Evaluate at the **start of each billing drain tick** (window = **1 Beat = 10 min**). Among billing drain candidates for that tick (up to batch size):
  - `attempts` = count of those candidates
  - `infra_failures` = candidates whose latest billing error is **infra/transport** (empty/missing queue URL, SQS/boto transport/timeout/connection) — **not** business-ineligible (`skipped`)
  - `rate = infra_failures / max(attempts, 1)`
  - **Open** if `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` **or** (`attempts >= MIN_SAMPLES` **and** `rate >= OPEN_RATE`) **or** (`infra_failures >= OPEN_ABS`)
  - **Clear path:** while open and pause is false, each tick with `attempts == 0` **or** `rate < CLEAR_RATE` increments a persisted clear streak; after **CLEAR_TICKS** consecutive healthy ticks → close. A bad tick resets the streak to 0.
  - Locked defaults (derived from Beat **10m** + `DRAIN_BATCH_SIZE` **100**):

  | Setting | Default | Derivation |
  |---------|---------|------------|
  | `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` | **false** | Manual override (also **v1** control) |
  | `CLOSE_PIPELINE_BILLING_OUTAGE_MIN_SAMPLES` | **10** | `BATCH/10` — avoid opening on 1/1 |
  | `CLOSE_PIPELINE_BILLING_OUTAGE_OPEN_RATE` | **0.5** | Majority infra-fail in the tick = brownout |
  | `CLOSE_PIPELINE_BILLING_OUTAGE_OPEN_ABS` | **50** | `BATCH/2` — half-batch infra fails |
  | `CLOSE_PIPELINE_BILLING_OUTAGE_CLEAR_RATE` | **0.2** | Hysteresis below open rate |
  | `CLOSE_PIPELINE_BILLING_OUTAGE_CLEAR_TICKS` | **2** | `2 × Beat` ≈ 20 min healthy before resume budget |

- Q: Where is automatic-circuit state stored (post-v1)? → **A:** **Redis** (same cache family as close-daily locks). Persist at least: `active` (bool), `clear_streak` (int), optional last `rate`/`attempts`/`infra_failures`. Key: `conversation_ms:close_pipeline:billing_outage`. Refresh TTL each drain tick (**86400s**). Not process-local memory. Does **not** reset Postgres `billing_reclaim_count`.
- Q: Datalake partial send when topics fails / goes `dead`? → **A:** **Expected.** Classification event may already be sent (`datalake_classification_at` set); topics event stays blocked while `topics_status ∉ {done, skipped}`. When topics is `dead`, datalake **stays** `pending` (partial) — MUST NOT auto-publish bias, MUST NOT auto-mark datalake `done`/`skipped`/`dead` solely because topics is `dead`. Recovery = fix/reclaim topics. Emit a metric/count for **datalake blocked by topics `dead`**.
- Q: Does every `dead` page Sentry per conversation? → **A:** **No.** Prefer aggregated metrics (`dead` counts by stage, rate of new `dead`, oldest pending age, pause/circuit flag, datalake-blocked-by-topics-dead). Sentry fingerprint by `stage + error_class`. Alert on **rate / spike**, not every row.

### Session 2026-08-07

- Q: Classify `dead` recovery vs Shape E (analysis I1)? → **A:** Shape E requires **no** `ClosePipelineRecord`. **Forbidden:** updating `Conversation.resolution` to terminal while a pipeline record still exists with `classify_status ∈ {pending, failed, dead}` (illegal hybrid). Recovery MUST be one of: (1) SM `dead → pending` + reclassify; or (2) SM `abandon_pipeline(record, resolution=…)` — deletes the `ClosePipelineRecord` (and related outbox if any) and MAY set terminal `resolution` in the same transaction → Shape E (or Shape A if left In Progress).
- Q: Classify `skipped` on close-daily (analysis I2)? → **A:** On the close-daily path, classify finishes **only** as `done` (chat room, no messages, Lambda success/unclassified). SM MUST NOT set `classify_status = skipped`. Enum/constraints MAY still allow `skipped` for symmetry; close-daily never uses it.
- Q: Heartbeat vs long Lambda / stale reclaim (analysis U2)? → **A:** Workers MUST refresh `{stage}_pending_at` at each attempt start **and** periodically during a long attempt at least every `CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS` (**600** = STALE/3). Soft time limits SHOULD be < stale TTL when catchable.
- Q: Topics already `skipped` at Shape C — datalake enqueue (analysis U3)? → **A:** First datalake run MUST publish **both** events when topics is already `skipped` (classification + topics bias). Enqueueing the topics Celery task after classify is a **safe no-op**. “Only classification until topics finishes” applies when topics is still `pending`/`failed`, not when initialized `skipped`.
- Q: US3 vs datalake stale when `classification_at` NULL (analysis I3)? → **A:** Drain MUST NOT stale-select datalake **solely by age** while waiting on the **topics event** (topics ∉ `{done, skipped}` **and** `datalake_classification_at` already set). Drain **MAY** reclaim datalake that is stale with `datalake_classification_at IS NULL` (or `failed`) even if topics is still open.
- Q: Automatic billing circuit in v1 (complexity)? → **A:** **Deferred post-v1.** v1 ships pause + bulk reopen + `dead`. Rate/Redis circuit design remains in research for a follow-up.

## User Scenarios & Testing *(mandatory)*
### User Story 1 — Know where close stopped (Priority: P1, Phase 1)

As an **engineer / on-call**, for any conversation that entered the close pipeline after deploy, I can read durable per-stage status, completion time, and failure error without guessing from logs alone.

**Why this priority**: Without durable stages, SIGKILL and partial failure are invisible after terminal resolution.

**Independent Test**: Persist rows through state machine; assert illegal combinations rejected by DB; backfilled legacy rows are all `done`.

**Acceptance Scenarios**:

1. **Given** an open conversation not yet claimed, **When** inspected, **Then** there is no `ClosePipelineRecord` and resolution is In Progress.
2. **Given** classify is `pending`, **When** inspected, **Then** a `ClosePipelineRecord` exists, topics/billing/datalake remain `NULL`, classify `pending_at` is set, and resolution is still In Progress.
3. **Given** classify commits successfully, **When** transaction completes, **Then** resolution is terminal, classify is `done` (never `skipped` on close-daily) with `*_at` set and `pending_at` NULL, topics is `pending` or `skipped`, billing is `pending` or `skipped` (business-ineligible only — never `skipped` for missing queue URL), and datalake is `pending` with both event ats NULL.
4. **Given** an attempt to mark billing `done` while classify is not finished, **When** persisted, **Then** rejected (application and/or DB).
5. **Given** a pre-feature terminal conversation, **When** backfill runs, **Then** a `ClosePipelineRecord` exists with all four stages `done`, `*_at` set, `pending_at` NULL, and empty errors (legacy assumed complete; drain will not replay).
6. **Given** a terminal conversation with no `ClosePipelineRecord` (out-of-band / Shape E), **When** drain runs, **Then** it is not selected as incomplete pipeline work.

---

### User Story 2 — Resume after failure without losing later stages (Priority: P1, Phase 2)

As the **platform**, after classify succeeds, topics and billing run as separate units of work; datalake events follow the DAG (classification after classify; topics after topics) so a kill or failure in one place does not erase progress elsewhere, and Billing is not blocked by topics.

**Why this priority**: Fixes the production failure mode (lost billing after classify) under Lambda single-flight constraints.

**Independent Test**: Simulate classify success then billing failure; assert billing stays `failed`/`pending` while classify remains `done`; topics failure leaves billing and classification-datalake runnable, but topics-datalake waits; topics `skipped` still publishes the topics datalake event (`bias` path).

**Acceptance Scenarios**:

1. **Given** eligible In Progress conversations for a project day, **When** the project selector runs, **Then** it creates/claims `ClosePipelineRecord` with classify `pending` + `pending_at`, sets close window end if needed, enqueues classify tasks, and does not invoke Lambdas or Billing inline.
2. **Given** a classify task, **When** it runs, **Then** it performs resolution only (not topics Lambda), commits classify + initializes topics/billing/datalake on the record atomically, and enqueues topics, billing, and datalake. If topics was initialized `skipped`, the first datalake run publishes **both** events; if topics is still open, datalake may send only classification until topics finishes.
3. **Given** topics Lambda fails, **When** topics is marked `failed`, **Then** billing can still complete and the classification datalake event can still be sent; the topics datalake event must not be sent until topics is `done` or `skipped`.
4. **Given** topics is initialized `skipped` at Shape C (no messages / no active topics), **When** datalake runs (first time), **Then** it publishes **both** classification and topics (bias) events, sets both event ats, and may mark datalake `done`; the topics Celery task is a safe no-op.
5. **Given** billing publish succeeds, **When** marked `done`, **Then** `billing_at` is set, `billing_pending_at` is NULL, error is empty; retries are no-ops.
6. **Given** Billing queue URL is empty/misconfigured, **When** the billing worker finishes retries, **Then** billing is `failed` (not `skipped`) with an error; after config fix, drain reclaims to `pending` and billing can complete.
7. **Given** classify and topics share the Lambda queue, **When** workers run, **Then** concurrency does not exceed the configured single-flight limit (no ThreadPool Lambda fan-out).
8. **Given** datalake classification event was recorded sent then the worker crashed, **When** datalake retries (and topics is finished), **Then** only the topics event is enqueued and classification is not sent again.
9. **Given** topics finishes `done`/`skipped` while classification datalake was already sent, **When** datalake runs, **Then** the topics event is handled (publish) and stage becomes `done` when both timestamps exist.

---

### User Story 3 — Automatic recovery of stuck stages (Priority: P2, Phase 3)

As the **platform**, abandoned `pending` stages (worker kill) and `failed` stages are reclaimed and requeued by a periodic drain without replaying legacy assumed-complete conversations; poison stages that exhaust reclaim budget become `dead` and stop automatic retry; shared infra brownouts MUST NOT mass-promote billing to `dead`.

**Why this priority**: Completes operational safety after cutover; can ship after Phase 2.

**Independent Test**: Force stale `pending` billing (`pending_at` older than TTL); run drain; assert task requeued and eventually `done` or `failed` with error. Force datalake `pending` with classification already sent and topics open; assert drain does **not** treat it as stale. Exhaust reclaim budget on a billing `failed` stage; assert status becomes `dead`. With billing pause on, assert reclaim does not increment toward `dead`. Force topics `dead` with classification datalake already sent; assert datalake stays pending partial.

**Acceptance Scenarios**:

1. **Given** classify `done` and billing `failed`, **When** drain runs and reclaim count is below max and billing pause is **off**, **Then** billing is reclaimed to `pending` (sets fresh `pending_at`, increments reclaim count) and requeued.
2. **Given** stale `pending` older than configured threshold (`pending_at` age) and reclaim count below max (and billing not pause-exempt), **When** drain runs, **Then** stage is re-enqueued without requiring a manual DB edit.
3. **Given** datalake `pending` with `datalake_classification_at` set and topics still `pending`/`failed`/`dead`, **When** drain runs, **Then** datalake is **not** selected as stale solely by age (waiting on topics event). **Given** datalake `pending`, stale, and `datalake_classification_at` IS NULL, **When** drain runs, **Then** datalake **may** be reclaimed even if topics is still open.
4. **Given** legacy all-`done` records or conversations with no `ClosePipelineRecord`, **When** drain runs, **Then** they are not selected for replay.
5. **Given** incomplete pipelines after classify, **When** operators query, **Then** they can list conversations with classify finished and billing not finished via `ClosePipelineRecord`.
6. **Given** a stage at `failed` (or stale `pending`) whose next reclaim would exceed `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` and the stage is **not** pause-exempt, **When** drain runs, **Then** the stage becomes `dead` with a non-empty error, is **not** re-enqueued, and subsequent drain runs leave it `dead`.
7. **Given** a stage in `dead`, **When** ops reclaim via state machine (single or bulk), **Then** status returns to `pending` with reclaim count reset and work may be enqueued again.
8. **Given** `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` is true, **When** drain runs on billing `failed` or stale `pending`, **Then** it MAY re-enqueue but MUST NOT increment `billing_reclaim_count` and MUST NOT mark billing `dead`.
9. **Given** topics is `dead` and `datalake_classification_at` is set with `datalake_topics_at` NULL, **When** drain runs, **Then** datalake remains `pending` (partial), is not stale-selected solely by age, and is not auto-completed via bias publish.
10. **Given** classify is `dead` (Shape B), **When** ops calls `abandon_pipeline` with a terminal resolution, **Then** the `ClosePipelineRecord` is deleted and resolution is terminal (Shape E); a raw resolution update without abandon is rejected / out of contract.

### Edge Cases

- Chat room short-circuit: classify finishes without resolution Lambda (`done`, resolution Has Chat Room); topics/billing initialized per skip/pending rules; datalake always `pending` in the same commit.
- No messages: `commit_classify_success` with resolution Unclassified (`3`), classify `done`; topics `skipped`; billing `pending` or `skipped` from payload/config rules; datalake `pending` — classification event still published; topics event still published via bias path after topics `skipped`.
- Conversation business-ineligible for Billing (cannot build a valid payload): billing `skipped` with `*_at` — intentional; not automatic drain. Ops may `skipped → pending` via state machine if the skip was wrong.
- Empty/missing Billing queue URL or transport failure after Celery retries: billing `failed` with error — **not** `skipped`. Drain recovers after config fix. Do not fail the whole classify stage for Billing config errors.
- No active topics for project: topics `skipped`; datalake topics event still published (bias path), same as today.
- Crash after Billing SQS accept but before mark `done`: residual race may cause a second publish; Billing upsert absorbs it as last-resort. Design goal: mark `done` immediately after successful publish under `select_for_update`; `done`/`skipped` are hard no-ops.
- Datalake outbox residual: publish succeeded then crash before `published_at` / conversation event-at persist — retry may republish once. UNIQUE outbox prevents a second *intent row*; it does not eliminate that residual. Accepted in v1; no datalake sink idempotency key required.
- Transient infra errors on classify: remain In Progress with classify `failed` or retryable `pending`; deterministic business unclassified outcomes commit terminal resolution.
- Concurrent classify claim on the same conversation: unique `ClosePipelineRecord` insert / `select_for_update` in `claim_classify` — only one transition Shape A→B succeeds.
- Terminal `resolution` set outside close-daily (admin/hotfix): no `ClosePipelineRecord` (Shape E). Drain ignores; close-daily must not leave this shape after classify on the new path.
- Phase 1 merged without Phase 2: new closes via old path leave Shape E (no record). Billing/datalake still run on the old path — tracking gap only. Prefer same-release cutover.
- Poison / permanently failing stage: after Celery retries → `failed`; after drain reclaim budget exhausted → `dead` (no further automatic enqueue). Not an SQS dead-letter queue. Applies to **poison**, not unchecked brownout (see billing outage mode).
- **Classify `dead` while In Progress (Shape B):** conversation remains In Progress; recovery only via SM `dead → pending` or SM `abandon_pipeline` (never raw resolution update with live record). No automatic Unclassified from classify `dead`. Downstream stages stay `NULL` until classify finishes or abandon.
- **Billing/SQS brownout (v1):** set `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE=true` so reclaim does not consume budget / promote `dead`; after fix, clear pause and bulk-reopen any premature `dead`. Automatic circuit is post-v1.
- **Classify/topics infra brownout (v1):** accepted risk of mass-`dead`; mitigate with pause drain / bulk reopen / fix infra — no Lambda circuit in v1.
- **Topics `dead` + datalake partial:** classification event may be sent; topics event blocked; datalake stays `pending` until topics is reclaimed to a finished state. No automatic bias publish from topics `dead`.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST persist independent stage status for classify, topics, billing, and datalake on a **`ClosePipelineRecord`** (OneToOne → `Conversation`) for each conversation that enters the close pipeline. `Conversation` MUST NOT gain these columns.
- **FR-002**: System MUST persist stage completed-at when status is `done` or `skipped`, MUST persist stage error when status is `failed` or `dead`, and MUST persist `{stage}_pending_at` when (and only when) status is `pending`, for every stage on `ClosePipelineRecord`.
- **FR-003**: System MUST reject illegal stage shapes on `ClosePipelineRecord` via CheckConstraints + state machine (see data-model.md). Terminal resolution **without** a pipeline record remains legal (Shape E); close-daily MUST NOT leave Shape E after classify on the new path.
- **FR-004**: System MUST initialize topics, billing, and datalake statuses on the same `ClosePipelineRecord` in the same commit that finishes classify successfully (Shape C). Datalake MUST be initialized as `pending` (never `skipped` at Shape C in v1).
- **FR-005**: System MUST follow the side-effect DAG: billing and the `conversation_classification` datalake event require classify finished only; the `topics` datalake event requires topics ∈ `{done, skipped}`. Topics failure MUST NOT block billing or the classification datalake event.
- **FR-006**: System MUST run classify and topics as separate units of work on the close path (topics MUST NOT be hidden inside the classify worker).
- **FR-007**: System MUST serialize Lambda-bound close work via a dedicated Celery queue with concurrency 1 for classify+topics (no ThreadPool Lambda fan-out). Per-conversation double-claim MUST be prevented with unique pipeline insert / `select_for_update` (see data-model.md). Deploy MUST reuse the existing Conversations Celery topology (same app/image); concurrency-1 MAY be a dedicated worker Deployment with the same image — MUST NOT require a new Argo application.
- **FR-008**: System MUST allow resume/retry of `pending` and `failed` stages without re-running finished stages. Celery may autoretry while status remains `pending`; the worker MUST mark `failed` when retries are exhausted or the error is non-retryable (see data-model Celery retry policy). Workers MUST heartbeat `{stage}_pending_at` at attempt start **and** at least every `CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS` (**600**) during long attempts. Drain performs `failed → pending` reclaim and re-enqueues stale `pending` while reclaim budget remains. SIGKILL leaves `pending` for drain. Drain MUST NOT automatically reclaim `dead`.
- **FR-019**: System MUST implement **logical dead letter**: when automatic drain would exceed `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` for a stage **and** the stage is not pause-exempt, it MUST mark that stage `dead` with a non-empty error and MUST NOT re-enqueue. State machine MUST support ops-only `dead → pending` (reset reclaim count), including a **bulk** reopen path. v1 MUST NOT add a separate SQS/Celery DLQ.
- **FR-020**: System MUST persist `{stage}_reclaim_count` (integer ≥ 0) per stage and MUST increment it on each automatic drain reclaim or stale re-enqueue that **consumes budget**. Drain MUST NOT increment `billing_reclaim_count` while `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE` is true.
- **FR-021**: System MUST expose locked **v1** defaults: classify/topics `max_retries=3`, billing/datalake `max_retries=5`, `CLOSE_PIPELINE_STALE_PENDING_SECONDS=1800`, `CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS=600`, drain Beat **10 minutes**, `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5`, `CLOSE_PIPELINE_DRAIN_BATCH_SIZE=100`, `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE=false`. Stale TTL MUST be ≥ Celery retry window. (Post-v1 circuit settings MIN_SAMPLES/OPEN_RATE/OPEN_ABS/CLEAR_RATE/CLEAR_TICKS/Redis TTL remain locked in Session 2026-08-05 but are not required to ship in v1.)
- **FR-022**: **v1 billing outage:** System MUST honor `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE`. While true, drain MAY re-enqueue billing but MUST NOT increment `billing_reclaim_count` and MUST NOT mark billing `dead`. The automatic rate/Redis circuit (Session 2026-08-05) is **deferred post-v1** — not a v1 MUST. Datalake/Lambda automatic circuits are NOT in v1.
- **FR-023**: When `topics_status = dead` and datalake is partial (`datalake_classification_at` set, `datalake_topics_at` NULL), system MUST leave datalake `pending`, MUST NOT auto-publish a topics bias event solely because topics is `dead`, and MUST expose a countable signal for datalake blocked by topics `dead`.
- **FR-024**: On close-daily, classify MUST finish only as `done` (SM MUST NOT set classify `skipped`). State machine MUST provide `abandon_pipeline` to delete `ClosePipelineRecord` (and related outbox) and optionally set terminal resolution → Shape E. Updating terminal `resolution` while Shape B record exists without `commit_classify_success` or `abandon_pipeline` is forbidden.
- **FR-017**: Billing MUST distinguish intentional `skipped` (business-ineligible payload) from recoverable `failed` (missing queue URL / transport). Config/infra MUST NOT initialize billing as `skipped`.
- **FR-018**: State machine MUST support ops-only `skipped → pending` reclaim for topics, billing, and datalake (not classify). Automatic drain MUST NOT reclaim `skipped`.
- **FR-009**: System MUST record billing delivery on `ClosePipelineRecord` (`billing_status` / `billing_at`) and MUST NOT re-publish when status is already `done` or `skipped`. Billing’s upsert behavior is a **consumer safety net only** — Conversations MUST NOT treat duplicate publish as a normal recovery strategy.
- **FR-010**: System MUST backfill pre-existing terminal conversations by creating `ClosePipelineRecord` as legacy assumed complete (all stages `done`) so automatic recovery does not mass-replay history.
- **FR-011**: System MUST provide periodic drain/reclaim for stuck or failed stages after cutover by querying `ClosePipelineRecord`. Stale `pending` MUST be detected via `{stage}_pending_at` age against `CLOSE_PIPELINE_STALE_PENDING_SECONDS`. Drain MUST process at most `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` per stage per run (default **100**). Drain MUST NOT stale-select datalake solely by age while waiting on the **topics event** (`datalake_classification_at` set and topics ∉ `{done, skipped}`). Drain MAY reclaim stale datalake with `datalake_classification_at IS NULL` or `datalake_status = failed` even if topics is still open. Drain MUST NOT create records for Shape E. Drain MUST NOT select `dead`/`skipped` for automatic reclaim.
- **FR-012**: System MUST emit enough structured observability to identify stage, project, and failure class, including aggregated counts of `dead` stages, rate of new `dead`, oldest non-terminal pending age, billing pause flag, and datalake-blocked-by-topics-dead. Alerts MUST prefer **rate/spike** signals over one Sentry event per conversation `dead`.
- **FR-013**: Adding a future fifth stage MUST follow the same status/at/error/pending_at/reclaim_count + transition + worker + drain pattern on `ClosePipelineRecord` without a new control-plane product design (normalized stage rows may be post-v1).
- **FR-014**: System MUST NOT intentionally produce duplicate datalake events. Implementation MUST use per-event sent timestamps on `ClosePipelineRecord` plus durable unique outbox `(conversation_id, event_kind)`. Residual single duplicate from publish-then-crash-before-mark is accepted and MUST be documented; sink idempotency keys are out of scope for v1.
- **FR-015**: System MUST enqueue topics and billing after classify success. System MUST enqueue datalake after classify and MUST re-enqueue datalake after topics finishes `done`/`skipped`, without re-sending an already-recorded event. When topics is **already** `skipped` at Shape C, the first datalake run MUST publish **both** events; topics task enqueue is a safe no-op.
- **FR-016**: When topics is `skipped`, datalake MUST still publish the topics event using the existing bias/empty-metadata builder (parity with current close-daily). Setting `datalake_topics_at` without an external publish is forbidden.

### Key Entities

- **Conversation**: Business entity; `resolution` only — **no** pipeline columns.
- **ClosePipelineRecord**: 1:1 control plane; per-stage status + completed-at + pending-at + error + reclaim count + datalake event ats.
- **CloseDatalakeOutbox**: Unique intent row per `(conversation_id, event_kind)` for datalake production.
- **Close pipeline state machine**: Only legal writer of pipeline fields; classify-commit, `dead`/reclaim, ops `abandon_pipeline`.
- **Stage worker**: Celery unit of work for one conversation and one stage (incl. periodic `pending_at` heartbeat).
- **Drain run**: Periodic reclaim/re-enqueue; promotes exhausted budget to `dead` except when billing pause exempts billing.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a forced worker kill following successful classify, billing (and other unfinished stages) remain visible as incomplete and can complete via retry/drain without manual SQL.
- **SC-002**: 100% of conversations that finish classify in the new pipeline have non-NULL topics, billing, and datalake stage statuses immediately after classify commit; datalake status is `pending`.
- **SC-003**: Illegal stage shapes (e.g. `done` without completed-at, billing finished while still In Progress, `pending` without `pending_at`, `dead` without error) cannot be persisted in automated tests.
- **SC-004**: Topics stage failure does not prevent billing stage completion or classification-datalake send in automated tests; topics-datalake remains blocked until topics finishes.
- **SC-005**: Close path does not use multi-worker/thread Lambda fan-out beyond configured single-flight concurrency.
- **SC-006**: On-call can answer “where did this conversation stop?” using `ClosePipelineRecord` stage status/at/error without reconstructing from Sentry alone for post-deploy conversations.
- **SC-007**: Legacy terminal conversations are not mass-reprocessed by drain after backfill.
- **SC-008**: After a crash following only the classification datalake enqueue, retry (once topics is finished) completes the topics event without re-enqueueing classification when the outbox/event-at already recorded success.
- **SC-009**: After classify success with topics still `pending`, datalake may record `datalake_classification_at` while leaving the stage `pending` until topics finishes and the topics event is published.
- **SC-010**: Topics `skipped` results in a published topics datalake event (bias path) and `datalake_topics_at` set — not a silent skip-without-publish.
- **SC-011**: Drain does not stale-select datalake solely by age when `datalake_classification_at` is set and topics ∉ `{done, skipped}`; may reclaim when `classification_at` IS NULL (or failed) even if topics is open.
- **SC-012**: Empty Billing queue URL results in billing `failed` (drain-recoverable), not `skipped`.
- **SC-013**: After Celery `max_retries` on a retryable error, the stage is `failed` with error and `pending_at` NULL.
- **SC-014**: `Conversation` has no close-pipeline columns; all stage fields live on `ClosePipelineRecord`.
- **SC-015**: After `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS` budget-consuming reclaim cycles (billing pause off), a still-unfinished stage is `dead` and further drain runs do not re-enqueue it.
- **SC-016**: Staging soak records whether yesterday’s claimed cohort reaches classify finished (or classify `dead`) within **12 hours** of claim; miss → capacity review, not silent ThreadPool.
- **SC-017**: With `CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE=true`, drain does **not** increment `billing_reclaim_count` toward `dead` and does **not** mark billing `dead`.
- **SC-018**: With topics `dead` and classification datalake already sent, datalake remains `pending` partial; blocked-by-topics-dead signal observable.
- **SC-019**: `abandon_pipeline` deletes the record (Shape E if terminal resolution set); raw terminal resolution update with live Shape B record is out of contract.
- **SC-020**: Close-daily classify commit always yields `classify_status=done` (never `skipped`); topics already `skipped` at Shape C → first datalake run publishes both events.
## Assumptions

- Resolution Lambda (and topics Lambda when sharing capacity) must be treated as single-flight for close-daily.
- Stage separation and durable status exist to handle edge cases maturely (resume the unfinished stage; never “just run everything again”).
- Billing **can** upsert if it receives a duplicate close message; Conversations still MUST avoid producing duplicates via stage idempotency (`done`/`skipped` ⇒ no-op). Upsert is not the primary recovery mechanism.
- Datalake does **not** deduplicate; Conversations MUST prevent intentional duplicate production via per-event sent tracking + unique outbox. Residual publish-then-crash window is accepted (documented), analogous to Billing.
- **Outbox cleanup/archival is deferred to post-v1**; ~2 rows per conversation may accumulate until then.
- Serial classify/topics **increases** wall-clock latency vs ThreadPool; accepted in exchange for correctness. Ops target: **12h** for classify completion of a claimed cohort (see Clarifications / SC-016). Raising Lambda parallelism requires models agreement.
- Dead letter is a **status on `ClosePipelineRecord`**, not a new queue or Argo service. It stops **poison** loops; billing brownout in **v1** is handled by **pause** (+ bulk reopen); automatic circuit is post-v1.
- Channel conversation count API is Billing’s tool and is not an acceptance gate for this pipeline.
- Message persist-before-classify and optional `migrate_messages` remain adjacent concerns wired during cutover/harden, not separate tracked stages in v1.
- Archive/retention Speckit patterns (state machine + constraints) are the reference implementation style; this feature does not depend on archive code being merged to main.
- Rare admin/hotfix terminal resolution outside close-daily may omit `ClosePipelineRecord`; that is Shape E. With a live Shape B record, ops MUST use `abandon_pipeline` (not raw resolution update).
- Topics datalake “bias” payload for skipped/no-classification cases remains the existing adapter contract (`build_topics_event`). Auto-bias from topics `dead` is forbidden.
- Preferred release: Phase 1 foundation and Phase 2 cutover in the **same deploy train**. Prefer shipping Phase 3 drain with billing **pause** (FR-022 v1); automatic circuit follow-up.
- **Accepted v1 risk:** classify/topics (and datalake transport) infra brownout may mass-`dead` those stages; mitigate with ops pause/bulk reopen.
- Concurrent duplicate Celery enqueues for the same conversation+stage are acceptable; workers no-op under `select_for_update` when already terminal or another attempt owns the transition (may be inefficient, not incorrect).
- Control plane lives on **`ClosePipelineRecord`**, not on `Conversation`, to keep the business model lean.
- Aggregated metrics/alerts are the primary signal for `dead` spikes; per-conversation Sentry flood is not an acceptance goal.
