# Feature Specification: Close-Daily Four-Stage Pipeline

**Feature Branch**: `001-close-daily-pipeline`

**Created**: 2026-07-28

**Status**: Draft

**Spec version**: 1.0.0

**Related artifacts**: [plan.md](./plan.md), [tasks.md](./tasks.md), [research.md](./research.md), [data-model.md](./data-model.md), [quickstart.md](./quickstart.md)

**Scope**: **nexus-conversations backend only** (no frontend, no Billing service changes, no reconcile against Billing totals).

**Input**: Restructure the daily conversation close flow so classify (resolution), topics, billing, and datalake are separately tracked stages with durable status, timestamps, and errors; illegal combinations are unrepresentable; Lambda work is single-flight; failures can resume without losing later stages.

## Context and motivation

The close-daily project Celery task currently runs resolution classification, topics classification, Billing SQS publish, and datalake event enqueue inside one long-lived worker (often with parallel classify threads). When the worker hits hard time limit (`SIGKILL`), conversations may already have a terminal `resolution` and will not be selected again—billing/datalake/topics can stop mid-flight with **no durable stage marker**.

Operators and engineers need to answer: **where did this conversation stop?** Extreme Billing resend is acceptable (Billing upserts). Delay from serial processing is acceptable. Cross-checking counts with Billing is **out of scope**.

Design principle: **make unreasonable states invalid** (same spirit as conversations S3 archive tracking): application state machine + database constraints so illegal status/timestamp/error/resolution combinations cannot persist.

## Glossary

| Term | Meaning |
|------|---------|
| **Resolution** | Business outcome on `Conversation.resolution` (`0` Resolved, `1` Unresolved, `2` In Progress, `3` Unclassified, `4` Has Chat Room) |
| **Close pipeline stage** | One of: classify, topics, billing, datalake |
| **Stage status** | `NULL` (not in pipeline) \| `pending` \| `done` \| `skipped` \| `failed` |
| **Stage completed-at** | Timestamp set only when status is `done` or `skipped` |
| **Stage error** | Non-empty text set only when status is `failed` |
| **Classify** | Resolution path (resolution Lambda or chat-room / no-messages short-circuit) that produces terminal resolution |
| **Topics** | Separate topics classifier Lambda + `ConversationClassification` persistence |
| **Billing stage** | Publish conversation-close message to Billing SQS |
| **Datalake stage** | Enqueue conversation_classification + topics datalake events |
| **Legacy assumed complete** | Backfill for conversations already terminal before this feature: all four stages marked `done` so drain does not replay history (not a verified send audit) |
| **Drain** | Periodic job that requeues `failed` or stale `pending` stages with valid preconditions |

## Program phases

| Phase | Backend deliverable |
|-------|---------------------|
| **1 — Foundation** | 12 stage columns, CheckConstraints, backfill, `ClosePipelineStateMachine`, tests; close runtime unchanged |
| **2 — Cutover** | Split classification APIs; four Celery stage workers; selector claim+enqueue; serial `close_lambda` queue; remove inline ThreadPool path |
| **3 — Drain & harden** | Drain beat, stale pending reclaim, metrics/Sentry per stage, selector lock/timeout tuning |

## Clarifications (locked)

### Session 2026-07-28

- Q: Separate Celery stages for classify vs topics? → **A:** Yes — two Lambdas, two stages, equal tracking fidelity.
- Q: Track only billing/datalake? → **A:** No — all four stages.
- Q: Gate billing on topics success? → **A:** No — billing/datalake require classify finished only; topics failure must not block Billing.
- Q: Lambda parallelism? → **A:** Single-flight; classify + topics share concurrency-1 queue; remove ThreadPool fan-out on close path.
- Q: Billing cross-check / reconcile? → **A:** Out of scope for this feature.
- Q: Historical conversations? → **A:** Backfill terminal rows as all-four `done` (*legacy assumed complete*); not a real send audit.
- Q: Extensibility? → **A:** New stage = copy columns + constraints + state-machine methods + worker + drain branch + enqueue edge.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Know where close stopped (Priority: P1, Phase 1)

As an **engineer / on-call**, for any conversation that entered the close pipeline after deploy, I can read durable per-stage status, completion time, and failure error without guessing from logs alone.

**Why this priority**: Without durable stages, SIGKILL and partial failure are invisible after terminal resolution.

**Independent Test**: Persist rows through state machine; assert illegal combinations rejected by DB; backfilled legacy rows are all `done`.

**Acceptance Scenarios**:

1. **Given** an open conversation not yet claimed, **When** inspected, **Then** all stage statuses are `NULL` and resolution is In Progress.
2. **Given** classify is `pending`, **When** inspected, **Then** topics/billing/datalake remain `NULL` and resolution is still In Progress.
3. **Given** classify commits successfully, **When** transaction completes, **Then** resolution is terminal, classify is `done` or `skipped` with `*_at` set, and topics/billing/datalake are each `pending` or `skipped` (never `NULL`).
4. **Given** an attempt to mark billing `done` while classify is not finished, **When** persisted, **Then** rejected (application and/or DB).
5. **Given** a pre-feature terminal conversation, **When** backfill runs, **Then** all four stages are `done` with `*_at` set and empty errors.

---

### User Story 2 — Resume after failure without losing later stages (Priority: P1, Phase 2)

As the **platform**, after classify succeeds, topics, billing, and datalake run as independent units of work so a kill or failure in one stage does not erase progress in others, and Billing is not blocked by topics.

**Why this priority**: Fixes the production failure mode (lost billing after classify) under Lambda single-flight constraints.

**Independent Test**: Simulate classify success then billing failure; assert billing stays `failed`/`pending` while classify remains `done`; topics failure leaves billing runnable.

**Acceptance Scenarios**:

1. **Given** eligible In Progress conversations for a project day, **When** the project selector runs, **Then** it claims classify (`pending`), sets close window end if needed, enqueues classify tasks, and does not invoke Lambdas or Billing inline.
2. **Given** a classify task, **When** it runs, **Then** it performs resolution only (not topics Lambda), commits classify + initializes downstream stages atomically, and enqueues pending topics/billing/datalake tasks.
3. **Given** topics Lambda fails, **When** topics is marked `failed`, **Then** billing and datalake can still complete.
4. **Given** billing publish succeeds, **When** marked `done`, **Then** `close_billing_at` is set and error is empty; retries are no-ops.
5. **Given** classify and topics share the Lambda queue, **When** workers run, **Then** concurrency does not exceed the configured single-flight limit (no ThreadPool Lambda fan-out).

---

### User Story 3 — Automatic recovery of stuck stages (Priority: P2, Phase 3)

As the **platform**, abandoned `pending` stages (worker kill) and `failed` stages are reclaimed and requeued by a periodic drain without replaying legacy assumed-complete conversations.

**Why this priority**: Completes operational safety after cutover; can ship after Phase 2.

**Independent Test**: Force stale `pending` billing; run drain; assert task requeued and eventually `done` or `failed` with error.

**Acceptance Scenarios**:

1. **Given** classify `done` and billing `failed`, **When** drain runs, **Then** billing is reclaimed to `pending` (if needed) and requeued.
2. **Given** stale `pending` older than configured threshold, **When** drain runs, **Then** stage is re-enqueued without requiring a manual DB edit.
3. **Given** legacy all-`done` or never-claimed `NULL` stages, **When** drain runs, **Then** they are not selected for replay.
4. **Given** incomplete pipelines after classify, **When** operators query, **Then** they can list conversations with classify finished and billing not finished.

---

### Edge Cases

- Chat room short-circuit: classify finishes without resolution Lambda; topics/billing/datalake still initialized per skip/pending rules.
- No messages: classify commits Unclassified (or agreed terminal outcome); does not leave In Progress without a stage outcome.
- Missing Billing payload fields or empty queue URL: billing stage `skipped` with `*_at`, not silent log-only drop.
- No active topics for project: topics `skipped`.
- Crash after Billing SQS accept but before mark `done`: extreme duplicate send acceptable; Conversations status remains source of truth after successful mark.
- Datalake two events: stage `done` only when both enqueues succeed; partial failure → `failed` or retryable `pending` per plan rules.
- Transient infra errors on classify: remain In Progress with classify `failed` or retryable `pending`; deterministic business unclassified outcomes commit terminal resolution.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST persist independent stage status for classify, topics, billing, and datalake on each conversation that enters the close pipeline.
- **FR-002**: System MUST persist stage completed-at when status is `done` or `skipped`, and MUST persist stage error when status is `failed`, for every stage.
- **FR-003**: System MUST reject illegal combinations of resolution and stage fields (make unreasonable states invalid) at the persistence boundary.
- **FR-004**: System MUST initialize topics, billing, and datalake statuses in the same commit that finishes classify successfully (no terminal resolution with downstream stages still `NULL` for pipeline-managed rows).
- **FR-005**: System MUST NOT require topics success before billing or datalake.
- **FR-006**: System MUST run classify and topics as separate units of work on the close path (topics MUST NOT be hidden inside the classify worker).
- **FR-007**: System MUST serialize Lambda-bound close work to respect single-flight capacity (no multi-thread Lambda fan-out on close).
- **FR-008**: System MUST allow resume/retry of `pending` and `failed` stages without re-running finished stages.
- **FR-009**: System MUST treat Billing Conversations-side stage status as source of truth for “sent”; extreme resend to Billing is allowed.
- **FR-010**: System MUST backfill pre-existing terminal conversations as legacy assumed complete (all stages `done`) so automatic recovery does not mass-replay history.
- **FR-011**: System MUST provide periodic drain/reclaim for stuck or failed stages after cutover.
- **FR-012**: System MUST emit enough structured observability to identify stage, conversation, project, and failure reason.
- **FR-013**: Adding a future fifth stage MUST follow the same status/at/error + transition + worker + drain pattern without a new control-plane design.

### Key Entities

- **Conversation**: Existing entity; gains pipeline control-plane fields per stage; `resolution` remains business outcome.
- **Close pipeline stage state**: Per-stage status + completed-at + error with strict shape rules.
- **Close pipeline state machine**: Only legal writer of stage fields; enforces transitions and classify-commit atomicity.
- **Stage worker**: Celery unit of work for one conversation and one stage.
- **Drain run**: Periodic selection of recoverable stage rows and re-enqueue.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a forced worker kill following successful classify, billing (and other unfinished stages) remain visible as incomplete and can complete via retry/drain without manual SQL.
- **SC-002**: 100% of conversations that finish classify in the new pipeline have non-NULL topics, billing, and datalake stage statuses immediately after classify commit.
- **SC-003**: Illegal stage shapes (e.g. `done` without completed-at, billing finished while still In Progress) cannot be persisted in automated tests.
- **SC-004**: Topics stage failure does not prevent billing stage completion in automated tests.
- **SC-005**: Close path does not use multi-worker/thread Lambda fan-out beyond configured single-flight concurrency.
- **SC-006**: On-call can answer “where did this conversation stop?” using stage status/at/error without reconstructing from Sentry alone for post-deploy conversations.
- **SC-007**: Legacy terminal conversations are not mass-reprocessed by drain after backfill.

## Assumptions

- Resolution Lambda (and topics Lambda when sharing capacity) must be treated as single-flight for close-daily.
- Billing accepts duplicate close messages and upserts by conversation identity.
- Delay from serial classify/topics is acceptable versus hard-timeout loss.
- Channel conversation count API is Billing’s tool and is not an acceptance gate for this pipeline.
- Message persist-before-classify and optional `migrate_messages` remain adjacent concerns wired during cutover/harden, not separate tracked stages in v1.
- Archive/retention Speckit patterns (state machine + constraints) are the reference implementation style; this feature does not depend on archive code being merged to main.
