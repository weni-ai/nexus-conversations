# Research: Close-Daily Four-Stage Pipeline

**Feature**: [spec.md](./spec.md) | **Date**: 2026-07-28

## R1 — Why four stages (not two)

**Decision**: Track classify, topics, billing, and datalake equally.

**Rationale**: Resolution and topics are separate Lambdas today inside one `ClassificationService.classify_conversation` call. Billing and datalake already run after resolution persist. Failure can occur in any of the four; tracking only side effects hides classify/topics failures and blocks extensibility.

**Alternatives rejected**: Stages only for billing/datalake; keep topics inside classify Celery task.

## R2 — Side-effect DAG (billing not gated on topics)

**Decision**: After classify finishes, enqueue **topics**, **billing**, and **datalake**. Billing and the `conversation_classification` datalake event depend only on classify. The `topics` datalake event depends on topics finishing (`done`/`skipped`). Topics failure must not block billing or the classification event.

```text
classify ──► billing
         └──► datalake event: conversation_classification

topics ──────► datalake event: topics
```

**Rationale**: Today billing does not wait for topics success. Gating billing on topics would couple revenue delivery to a second Lambda. Classification analytics can leave as soon as resolution is terminal; topics analytics must wait for the topics stage to finish (including `skipped`, which still emits the bias topics event).

**Alternatives rejected**: `classify → topics → (billing ∥ datalake)` (blocks billing on topics); fire both datalake events only after both classify+topics (delays classification analytics); split into two full Celery stages for datalake (extra control-plane without clearer ops).

## R3 — Make unreasonable states invalid

**Decision**: Mirror archive pattern: state machine + CheckConstraints on status/at/pending_at/error and lifecycle precedence.

**Rationale**: Independent booleans (`billing_sent=true` while In Progress) are the failure mode. Archive already proved the pattern in this codebase (`ArchiveRecordStateMachine`, archive record constraints).

## R4 — Columns on Conversation vs separate CloseRecord

**Decision**: Eighteen pipeline columns on `Conversation` (status + at + pending_at + error × 4 stages + two datalake event ats), plus `CloseDatalakeOutbox` with UNIQUE `(conversation_id, event_kind)`. No separate `CloseRecord` lifecycle entity.

**Rationale**: Close does not delete the conversation row; drain queries stay simple; `pending_at` is the only honest stale clock; outbox is producer machinery for the two datalake events, not a fifth product stage.

## R5 — Legacy backfill as done

**Decision**: Terminal pre-feature rows → all stages `done` (*legacy assumed complete*).

**Rationale**: Enables strong constraints and prevents drain mass-replay. Not a verified Billing/datalake audit. Alternative (leave NULL forever) is more honest about history but weakens “terminal ⇒ stages set” invariants; team locked backfill.

## R6 — Lambda concurrency

**Decision**: Dedicated `close_lambda` queue, worker concurrency 1, shared by classify + topics; remove close-daily ThreadPool Lambda fan-out.

**Rationale**: Resolution Lambda does not accept meaningful parallel processing; topics shares the same operational constraint when capacity is shared.

## R7 — Pattern reference without depending on archive merge

**Decision**: Copy design ideas from archive Speckit/state machine; implement close_daily modules independently so this stack can merge from main without waiting for archive PRs.

**Rationale**: Archive Speckit lives on feature branches; main may not have archive code when close-daily ships.

## R8 — Static CheckConstraints vs “terminal ⇒ stages filled”

**Decision**: Do **not** DB-enforce `resolution ≠ IN_PROGRESS ⇒ stages NOT NULL`. Do enforce shape + precedence constraints that are always true. Infer pipeline membership from `close_classify_status IS NOT NULL`. Allow terminal + all-NULL as out-of-band.

**Rationale**: Django CheckConstraints are not phased by deploy. Forcing terminal⇒stages breaks legitimate out-of-band resolution updates and conflates backfill with runtime. Close-daily still always writes Shape C via the state machine; backfill still marks legacy terminals `done` for anti-replay.

**Alternatives rejected**: `pipeline_managed` boolean (extra column, same inference as classify status non-NULL); soft-only constraints with no DB shape checks.

## R9 — Datalake must not intentionally duplicate

**Decision**: Per-event sent timestamps on `Conversation` **plus** durable outbox UNIQUE `(conversation_id, event_kind)` for `{classification, topics}`. Publish only unpublished outbox rows; project success onto `close_datalake_*_at` and `published_at` together after publish OK. Mark stage `done` when both timestamps are set. Reclaim clears error only.

**Rationale**: Datalake creates real duplicates. Producer-side unique intent is mandatory. UNIQUE closes the “second intent” crash window; it does **not** remove the residual window after external publish succeeds and before DB mark — that residual is accepted (R12).

**Alternatives rejected**: Accept duplicates as normal; timestamps only; rely on datalake-side dedup; two full Celery stages without outbox; require sink idempotency keys in v1.

## R10 — Single-flight vs double-claim

**Decision**: Queue concurrency 1 for Lambda serialization; `select_for_update` for per-conversation claim/transition races.

**Rationale**: These are different hazards. Concurrency 1 does not stop two tasks from targeting the same uuid if both were enqueued; row locks do.

## R11 — Billing upsert is a safety net, not a strategy

**Decision**: Billing workers MUST be idempotent on Conversations state (`done`/`skipped` ⇒ no publish). Do not “fix incompleteness” by re-sending to Billing as a matter of course. Billing’s ability to upsert the same conversation close is documented only as protection against a residual crash window after SQS accept and before `mark done`.

**Rationale**: The point of stage tracking and separation is mature edge-case handling — know where we stopped and continue from there — not to lean on downstream dedup. Datalake has no such net; Billing’s net must not become an excuse for sloppy retries.

## R12 — Outbox residual window (honesty)

**Decision**: Document and accept that `publish → crash → mark published` can still yield one duplicate datalake event. Do not claim true end-to-end exactly-once. Do not require datalake sink idempotency keys in v1.

**Rationale**: Same residual class as Billing. Closing it fully needs sink-side dedup or a synchronous confirm API we do not have. Unique outbox + conversation event-ats remain the control plane against *intentional* and *multi-intent* duplicates.

## R13 — Topics skipped still publishes topics datalake event

**Decision**: When topics stage is `skipped` (no messages / no active topics), datalake MUST still publish the topics event via existing `build_topics_event` bias/empty-metadata path. Forbidden: set `close_datalake_topics_at` without publish.

**Rationale**: Matches today’s `_send_datalake_events` behavior (always sends both events; bias when no classification / no active topics). Skipping the Lambda is not the same as omitting analytics.

## R14 — Stale pending clock = `*_pending_at`

**Decision**: Per-stage `close_{stage}_pending_at`; set on enter `pending`, cleared on leave. Drain TTL compares against this column only.

**Rationale**: Conversation `updated_at` moves for unrelated writes; `close_{stage}_at` is NULL while pending by shape rules. A dedicated pending clock keeps constraints honest.

**Alternatives rejected**: use `updated_at`; overload `*_at` while pending; Redis-only claim TTL as sole drain clock.

## R15 — Datalake never skipped at Shape C (v1)

**Decision**: After classify success, datalake always initializes as `pending`. Topics may init as `skipped`. Billing may init as `skipped` only when business-ineligible.

**Rationale**: Close-daily always emits datalake events today; there is no project “datalake off” switch in scope. A skipped datalake init path would be speculative and confuse ops.

## R16 — Drain must not stale-spin datalake waiting on topics

**Decision**: Datalake `pending` is not stale-eligible solely by age while topics ∉ `{done, skipped}`.

**Rationale**: After classification is sent, datalake legitimately waits for topics to finish before the topics event. Age-based requeue would only produce no-ops and noise.

## R17 — Billing skipped vs failed

**Decision**: Business-ineligible payload → `skipped`. Empty/missing queue URL and transport failures → `failed` (drain-recoverable). Ops-only `skipped → pending` exists; drain never auto-reclaims `skipped`.

**Rationale**: Using `skipped` for deploy misconfig permanently loses billing (no automatic path back). Failing classify for Billing config would couple resolution to Billing infra wrongly.

## R18 — Celery pending → failed ownership

**Decision**: Stage task marks `failed` after `max_retries` or non-retryable error; heartbeat refreshes `pending_at` each attempt; SIGKILL → drain stale path. Defaults: max_retries=5, stale TTL ≥ retry window (suggest 3600s).

**Rationale**: Removes ambiguity between Celery autoretry and drain reclaim; prevents double-queue while retries are healthy.

## R19 — Phase 1→2 Shape E gap

**Decision**: Prefer same release train for foundation+cutover. If gap exists: old path still delivers billing/datalake; Shape E is tracking-only; no v1 backfill of gap Shape E into the new pipeline.

**Rationale**: Phase 1 explicitly leaves runtime unchanged — delivery is not lost. Inventing a mid-gap dual-write increases cutover risk for little gain.

## R20 — Outbox growth deferred

**Decision**: No cleanup/archival in v1; document post-v1 follow-up.

**Rationale**: Correctness first; ~2 rows/conversation is acceptable until volume justifies a retention job.
