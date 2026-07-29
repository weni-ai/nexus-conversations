# Feature Specification: Close-Daily Four-Stage Pipeline

**Feature Branch**: `001-close-daily-pipeline`

**Created**: 2026-07-28

**Status**: Draft

**Spec version**: 1.4.0

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
| **Stage status** | `NULL` (stage not started on the record) \| `pending` \| `done` \| `skipped` \| `failed` |
| **Stage completed-at** | Timestamp set only when status is `done` or `skipped` (`{stage}_at` on `ClosePipelineRecord`) |
| **Stage pending-at** | Timestamp set only while status is `pending` (`{stage}_pending_at`); drain stale clock |
| **Stage error** | Non-empty text set only when status is `failed` |
| **Classify** | Resolution path (resolution Lambda or chat-room / no-messages short-circuit) that produces terminal resolution |
| **Topics** | Separate topics classifier Lambda + `ConversationClassification` persistence |
| **Billing stage** | Publish conversation-close message to Billing SQS |
| **Datalake stage** | Orchestrates **two** events with different preconditions: `conversation_classification` after classify; `topics` after topics finishes (`done` **or** `skipped`). No intentional duplicates; each event has its own durable sent timestamp |
| **Legacy assumed complete** | Backfill for conversations already terminal before this feature: create `ClosePipelineRecord` with all four stages `done` so drain does not replay history (not a verified send audit) |
| **Drain** | Periodic job that requeues `failed` or stale `pending` stages on existing `ClosePipelineRecord`s with valid preconditions |

## Program phases

| Phase | Backend deliverable |
|-------|---------------------|
| **1 — Foundation** | `ClosePipelineRecord` (18 stage fields) + `CloseDatalakeOutbox`, CheckConstraints, backfill, `ClosePipelineStateMachine`, tests; close runtime unchanged |
| **2 — Cutover** | Split classification APIs; four Celery stage workers; selector claim+enqueue; serial `close_lambda` queue; remove inline ThreadPool path |
| **3 — Drain & harden** | Drain beat on `ClosePipelineRecord`, stale pending reclaim via `*_pending_at`, metrics/Sentry per stage, selector lock/timeout tuning |

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

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Know where close stopped (Priority: P1, Phase 1)

As an **engineer / on-call**, for any conversation that entered the close pipeline after deploy, I can read durable per-stage status, completion time, and failure error without guessing from logs alone.

**Why this priority**: Without durable stages, SIGKILL and partial failure are invisible after terminal resolution.

**Independent Test**: Persist rows through state machine; assert illegal combinations rejected by DB; backfilled legacy rows are all `done`.

**Acceptance Scenarios**:

1. **Given** an open conversation not yet claimed, **When** inspected, **Then** there is no `ClosePipelineRecord` and resolution is In Progress.
2. **Given** classify is `pending`, **When** inspected, **Then** a `ClosePipelineRecord` exists, topics/billing/datalake remain `NULL`, classify `pending_at` is set, and resolution is still In Progress.
3. **Given** classify commits successfully, **When** transaction completes, **Then** resolution is terminal, classify is `done` or `skipped` with `*_at` set and `pending_at` NULL, topics is `pending` or `skipped`, billing is `pending` or `skipped` (business-ineligible only — never `skipped` for missing queue URL), and datalake is `pending` with both event ats NULL.
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
2. **Given** a classify task, **When** it runs, **Then** it performs resolution only (not topics Lambda), commits classify + initializes topics/billing/datalake on the record atomically, and enqueues topics, billing, and datalake (datalake may send only the classification event until topics finishes).
3. **Given** topics Lambda fails, **When** topics is marked `failed`, **Then** billing can still complete and the classification datalake event can still be sent; the topics datalake event must not be sent until topics is `done` or `skipped`.
4. **Given** topics is `skipped` (no messages / no active topics), **When** datalake runs, **Then** it publishes the topics event via the existing bias/empty-metadata builder, sets `datalake_topics_at`, and does not invent a silent skip-without-publish.
5. **Given** billing publish succeeds, **When** marked `done`, **Then** `billing_at` is set, `billing_pending_at` is NULL, error is empty; retries are no-ops.
6. **Given** Billing queue URL is empty/misconfigured, **When** the billing worker finishes retries, **Then** billing is `failed` (not `skipped`) with an error; after config fix, drain reclaims to `pending` and billing can complete.
7. **Given** classify and topics share the Lambda queue, **When** workers run, **Then** concurrency does not exceed the configured single-flight limit (no ThreadPool Lambda fan-out).
8. **Given** datalake classification event was recorded sent then the worker crashed, **When** datalake retries (and topics is finished), **Then** only the topics event is enqueued and classification is not sent again.
9. **Given** topics finishes `done`/`skipped` while classification datalake was already sent, **When** datalake runs, **Then** the topics event is handled (publish) and stage becomes `done` when both timestamps exist.

---

### User Story 3 — Automatic recovery of stuck stages (Priority: P2, Phase 3)

As the **platform**, abandoned `pending` stages (worker kill) and `failed` stages are reclaimed and requeued by a periodic drain without replaying legacy assumed-complete conversations.

**Why this priority**: Completes operational safety after cutover; can ship after Phase 2.

**Independent Test**: Force stale `pending` billing (`pending_at` older than TTL); run drain; assert task requeued and eventually `done` or `failed` with error. Force datalake `pending` waiting on topics; assert drain does **not** treat it as stale.

**Acceptance Scenarios**:

1. **Given** classify `done` and billing `failed`, **When** drain runs, **Then** billing is reclaimed to `pending` (sets fresh `pending_at`) and requeued.
2. **Given** stale `pending` older than configured threshold (`pending_at` age), **When** drain runs, **Then** stage is re-enqueued without requiring a manual DB edit.
3. **Given** datalake `pending` with topics still `pending`/`failed`, **When** drain runs, **Then** datalake is **not** selected as stale solely by age (still waiting on topics precondition for the topics event).
4. **Given** legacy all-`done` records or conversations with no `ClosePipelineRecord`, **When** drain runs, **Then** they are not selected for replay.
5. **Given** incomplete pipelines after classify, **When** operators query, **Then** they can list conversations with classify finished and billing not finished via `ClosePipelineRecord`.

---

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

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST persist independent stage status for classify, topics, billing, and datalake on a **`ClosePipelineRecord`** (OneToOne → `Conversation`) for each conversation that enters the close pipeline. `Conversation` MUST NOT gain these columns.
- **FR-002**: System MUST persist stage completed-at when status is `done` or `skipped`, MUST persist stage error when status is `failed`, and MUST persist `{stage}_pending_at` when (and only when) status is `pending`, for every stage on `ClosePipelineRecord`.
- **FR-003**: System MUST reject illegal stage shapes on `ClosePipelineRecord` via CheckConstraints + state machine (see data-model.md). Terminal resolution **without** a pipeline record remains legal (Shape E); close-daily MUST NOT leave Shape E after classify on the new path.
- **FR-004**: System MUST initialize topics, billing, and datalake statuses on the same `ClosePipelineRecord` in the same commit that finishes classify successfully (Shape C). Datalake MUST be initialized as `pending` (never `skipped` at Shape C in v1).
- **FR-005**: System MUST follow the side-effect DAG: billing and the `conversation_classification` datalake event require classify finished only; the `topics` datalake event requires topics ∈ `{done, skipped}`. Topics failure MUST NOT block billing or the classification datalake event.
- **FR-006**: System MUST run classify and topics as separate units of work on the close path (topics MUST NOT be hidden inside the classify worker).
- **FR-007**: System MUST serialize Lambda-bound close work via a dedicated Celery queue with concurrency 1 for classify+topics (no ThreadPool Lambda fan-out). Per-conversation double-claim MUST be prevented with unique pipeline insert / `select_for_update` (see data-model.md).
- **FR-008**: System MUST allow resume/retry of `pending` and `failed` stages without re-running finished stages. Celery may autoretry while status remains `pending`; the worker MUST mark `failed` when retries are exhausted or the error is non-retryable (see data-model Celery retry policy). Drain performs `failed → pending` reclaim and re-enqueues stale `pending`. SIGKILL leaves `pending` for drain.
- **FR-017**: Billing MUST distinguish intentional `skipped` (business-ineligible payload) from recoverable `failed` (missing queue URL / transport). Config/infra MUST NOT initialize billing as `skipped`.
- **FR-018**: State machine MUST support ops-only `skipped → pending` reclaim for topics, billing, and datalake. Automatic drain MUST NOT reclaim `skipped`.
- **FR-009**: System MUST record billing delivery on `ClosePipelineRecord` (`billing_status` / `billing_at`) and MUST NOT re-publish when status is already `done` or `skipped`. Billing’s upsert behavior is a **consumer safety net only** — Conversations MUST NOT treat duplicate publish as a normal recovery strategy.
- **FR-010**: System MUST backfill pre-existing terminal conversations by creating `ClosePipelineRecord` as legacy assumed complete (all stages `done`) so automatic recovery does not mass-replay history.
- **FR-011**: System MUST provide periodic drain/reclaim for stuck or failed stages after cutover by querying `ClosePipelineRecord`. Stale `pending` MUST be detected via `{stage}_pending_at` age against `CLOSE_PIPELINE_STALE_PENDING_SECONDS`. Drain MUST process at most `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` per stage per run (default **100**). Drain MUST NOT treat datalake as stale solely by age while topics ∉ `{done, skipped}`. Drain MUST NOT create records for Shape E.
- **FR-012**: System MUST emit enough structured observability to identify stage, conversation, project, and failure reason.
- **FR-013**: Adding a future fifth stage MUST follow the same status/at/error/pending_at + transition + worker + drain pattern on `ClosePipelineRecord` without a new control-plane product design (normalized stage rows may be post-v1).
- **FR-014**: System MUST NOT intentionally produce duplicate datalake events. Implementation MUST use per-event sent timestamps on `ClosePipelineRecord` plus durable unique outbox `(conversation_id, event_kind)`. Residual single duplicate from publish-then-crash-before-mark is accepted and MUST be documented; sink idempotency keys are out of scope for v1.
- **FR-015**: System MUST enqueue topics and billing after classify success. System MUST enqueue datalake after classify (classification event) and MUST re-enqueue datalake after topics finishes `done`/`skipped` (topics event), without re-sending an already-recorded event.
- **FR-016**: When topics is `skipped`, datalake MUST still publish the topics event using the existing bias/empty-metadata builder (parity with current close-daily). Setting `datalake_topics_at` without an external publish is forbidden.

### Key Entities

- **Conversation**: Business entity; `resolution` only — **no** pipeline columns.
- **ClosePipelineRecord**: 1:1 control plane; per-stage status + completed-at + pending-at + error + datalake event ats.
- **CloseDatalakeOutbox**: Unique intent row per `(conversation_id, event_kind)` for datalake production.
- **Close pipeline state machine**: Only legal writer of pipeline fields; enforces transitions and classify-commit atomicity.
- **Stage worker**: Celery unit of work for one conversation and one stage.
- **Drain run**: Periodic selection of recoverable `ClosePipelineRecord` rows and re-enqueue.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a forced worker kill following successful classify, billing (and other unfinished stages) remain visible as incomplete and can complete via retry/drain without manual SQL.
- **SC-002**: 100% of conversations that finish classify in the new pipeline have non-NULL topics, billing, and datalake stage statuses immediately after classify commit; datalake status is `pending`.
- **SC-003**: Illegal stage shapes (e.g. `done` without completed-at, billing finished while still In Progress, `pending` without `pending_at`) cannot be persisted in automated tests.
- **SC-004**: Topics stage failure does not prevent billing stage completion or classification-datalake send in automated tests; topics-datalake remains blocked until topics finishes.
- **SC-005**: Close path does not use multi-worker/thread Lambda fan-out beyond configured single-flight concurrency.
- **SC-006**: On-call can answer “where did this conversation stop?” using `ClosePipelineRecord` stage status/at/error without reconstructing from Sentry alone for post-deploy conversations.
- **SC-007**: Legacy terminal conversations are not mass-reprocessed by drain after backfill.
- **SC-008**: After a crash following only the classification datalake enqueue, retry (once topics is finished) completes the topics event without re-enqueueing classification when the outbox/event-at already recorded success.
- **SC-009**: After classify success with topics still `pending`, datalake may record `datalake_classification_at` while leaving the stage `pending` until topics finishes and the topics event is published.
- **SC-010**: Topics `skipped` results in a published topics datalake event (bias path) and `datalake_topics_at` set — not a silent skip-without-publish.
- **SC-011**: Drain does not select datalake as stale-pending while topics ∉ `{done, skipped}`.
- **SC-012**: Empty Billing queue URL results in billing `failed` (drain-recoverable), not `skipped`.
- **SC-013**: After Celery `max_retries` on a retryable error, the stage is `failed` with error and `pending_at` NULL.
- **SC-014**: `Conversation` has no close-pipeline columns; all stage fields live on `ClosePipelineRecord`.

## Assumptions

- Resolution Lambda (and topics Lambda when sharing capacity) must be treated as single-flight for close-daily.
- Stage separation and durable status exist to handle edge cases maturely (resume the unfinished stage; never “just run everything again”).
- Billing **can** upsert if it receives a duplicate close message; Conversations still MUST avoid producing duplicates via stage idempotency (`done`/`skipped` ⇒ no-op). Upsert is not the primary recovery mechanism.
- Datalake does **not** deduplicate; Conversations MUST prevent intentional duplicate production via per-event sent tracking + unique outbox. Residual publish-then-crash window is accepted (documented), analogous to Billing.
- **Outbox cleanup/archival is deferred to post-v1**; ~2 rows per conversation may accumulate until then.
- Delay from serial classify/topics is acceptable versus hard-timeout loss; **no numeric throughput SLA** is in scope for v1.
- Channel conversation count API is Billing’s tool and is not an acceptance gate for this pipeline.
- Message persist-before-classify and optional `migrate_messages` remain adjacent concerns wired during cutover/harden, not separate tracked stages in v1.
- Archive/retention Speckit patterns (state machine + constraints) are the reference implementation style; this feature does not depend on archive code being merged to main.
- Rare admin/hotfix terminal resolution outside close-daily may omit `ClosePipelineRecord`; that is Shape E (“not pipeline-managed”), not drain backlog.
- Topics datalake “bias” payload for skipped/no-classification cases remains the existing adapter contract (`build_topics_event`).
- Preferred release: Phase 1 foundation and Phase 2 cutover in the **same deploy train** to minimize Shape E tracking gaps; old path still delivers side effects if a gap exists.
- Concurrent duplicate Celery enqueues for the same conversation+stage are acceptable; workers no-op under `select_for_update` when already terminal or another attempt owns the transition (may be inefficient, not incorrect).
- Control plane lives on **`ClosePipelineRecord`**, not on `Conversation`, to keep the business model lean.
