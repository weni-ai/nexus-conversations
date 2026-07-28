# Implementation Plan: Close-Daily Four-Stage Pipeline

**Branch**: `001-close-daily-pipeline` | **Date**: 2026-07-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-close-daily-pipeline/spec.md`

## Summary

Replace the monolithic per-project close-daily batch (classify + topics + billing + datalake in one long Celery task with ThreadPool Lambda fan-out) with a **four-stage pipeline** on `Conversation`: classify, topics, billing, datalake. Each stage uses `status` + `*_at` + `*_pending_at` + `*_error`. Datalake adds **per-event sent timestamps** + `CloseDatalakeOutbox` (UNIQUE conversation+event_kind) so retries do not create a second intent. Side-effect DAG: classify → billing and classification-datalake; topics → topics-datalake (publish even when topics is `skipped`, bias path). Mutations go only through `ClosePipelineStateMachine`. DB `CheckConstraint`s make illegal combinations unrepresentable. Classify and topics run on a concurrency-1 Lambda queue. Drain recovers `failed` / stale `pending` via `*_pending_at`, without stale-spinning datalake that is only waiting on topics. Legacy terminal rows are backfilled as all-`done` (*legacy assumed complete*).

## Technical Context

**Language/Version**: Python 3.11 (project Poetry)

**Primary Dependencies**: Django, Celery, Redis (locks/cache), boto3 (Lambda), existing SQS billing producer, existing datalake Celery events / `build_*_event` adapters

**Storage**: PostgreSQL (`Conversation` / `intelligences_conversation` + outbox table)

**Testing**: pytest / Django TestCase patterns in `conversation_ms/tests/`

**Target Platform**: nexus-conversations MS (Celery workers + Beat)

**Project Type**: backend microservice

**Performance Goals**: Correctness and resumability over wall-clock speed; serial Lambda accepted

**Constraints**: Resolution Lambda single-flight; no Billing reconcile; no raise hard time limit as primary fix; no sink idempotency keys for datalake in v1

**Scale/Scope**: All projects processed by close-daily; per-conversation stage tasks after selector

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status |
|-----------|--------|
| I. Service Boundary — backend-only in nexus-conversations | Pass |
| II. Data Safety — no destructive delete of conversations in this feature | Pass (N/A destructive) |
| III. Batch Job Patterns — Celery + Redis locks; selector keeps close_daily lock model, stage workers are per-conversation | Pass with adaptation |
| IV. Test Coverage — constraints, state machine, tasks, drain | Pass (required in tasks) |
| V. Observability — structured logs + Sentry per stage | Pass |
| VI. Configuration Over Hardcoding — queue names, stale TTL, drain batch via settings | Pass |

## Project Structure

### Documentation (this feature)

```text
specs/001-close-daily-pipeline/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── spec.md
├── tasks.md
└── checklists/requirements.md
```

### Source Code (expected touchpoints)

```text
conversation_ms/
├── models.py                          # 18 pipeline columns + CheckConstraints + CloseDatalakeOutbox
├── migrations/00xx_close_pipeline_*.py
├── close_daily/
│   ├── constants.py                   # stage enum, queue names, event kinds, stale TTL setting name
│   ├── state_machine.py               # NEW
│   ├── metrics.py                     # NEW
│   ├── drain.py                       # NEW (Phase 3; pending_at + datalake wait rule)
│   ├── runner.py                      # selector-only after cutover
│   └── stages/                        # NEW workers (Phase 2; datalake respects DAG)
├── services/classification_service.py # split resolution vs topics
├── adapters/data_lake.py              # reuse build_*_event (bias path for skipped topics)
├── tasks.py                           # stage tasks + drain
└── tests/test_close_pipeline_*.py
nexus_conversations/
├── settings.py                        # CLOSE_PIPELINE_* incl. STALE_PENDING_SECONDS
└── celery.py                          # beat + routes
```

## Complexity Tracking

| Decision | Why needed |
|----------|------------|
| 18 pipeline columns vs JSON blob | Enables CheckConstraints, drain indexes, clear ops queries; pending_at + 2 event ats |
| Billing not gated on topics | Avoid recreating Billing lag on topics Lambda failure |
| Datalake events follow DAG | Classification after classify; topics event after topics done/skipped |
| Topics skipped still publishes | Parity with today’s bias topics event |
| Billing upsert = safety net only | Stage idempotency is the real control; do not design for duplicate publishes |
| Outbox residual accepted | Honest about publish→mark crash; no sink idempotency in v1 |
| Legacy backfill as `done` | Anti-replay for drain; not a send audit |
| No DB rule `terminal ⇒ stages filled` | Constraints must be static; out-of-band closes stay legal (Shape E) |
| Shared `close_lambda` concurrency 1 | Lambda single-flight |
| `select_for_update` on claim/transition | Prevent double-claim / torn stage updates |
| `*_pending_at` stale clock | Only honest pending age under shape constraints |
| Drain skips datalake waiting on topics | Avoid no-op stale requeues |
| Datalake never skipped at Shape C | Always emit path in v1; no speculative “datalake off” |
