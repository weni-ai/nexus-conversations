# Implementation Plan: Close-Daily Four-Stage Pipeline

**Branch**: `001-close-daily-pipeline` | **Date**: 2026-07-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-close-daily-pipeline/spec.md`

## Summary

Replace the monolithic per-project close-daily batch (classify + topics + billing + datalake in one long Celery task with ThreadPool Lambda fan-out) with a **four-stage pipeline** tracked on **`ClosePipelineRecord`** (OneToOne → `Conversation`): classify, topics, billing, datalake. Each stage uses `status` + `*_at` + `*_pending_at` + `*_error`. Datalake adds **per-event sent timestamps** + `CloseDatalakeOutbox` (UNIQUE conversation+event_kind). Side-effect DAG: classify → billing and classification-datalake; topics → topics-datalake (publish even when topics is `skipped`, bias path). Mutations go only through `ClosePipelineStateMachine`. DB `CheckConstraint`s on the pipeline record make illegal stage shapes unrepresentable. Classify and topics run on a concurrency-1 Lambda queue. Drain recovers `failed` / stale `pending` via `*_pending_at`, without stale-spinning datalake that is only waiting on topics. Legacy terminal rows get backfilled `ClosePipelineRecord`s as all-`done` (*legacy assumed complete*). `Conversation` stays free of pipeline columns.

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
├── models.py                          # ClosePipelineRecord + CloseDatalakeOutbox (+ Conversation unchanged for pipeline)
├── migrations/00xx_close_pipeline_*.py
├── close_daily/
│   ├── constants.py                   # stage enum, queue names, event kinds, stale TTL / drain batch / celery retry settings
│   ├── state_machine.py               # NEW (mutates ClosePipelineRecord; ops skipped→pending)
│   ├── metrics.py                     # NEW
│   ├── drain.py                       # NEW (Phase 3; queries ClosePipelineRecord)
│   ├── runner.py                      # selector-only after cutover
│   └── stages/                        # NEW workers (Phase 2; datalake respects DAG; Celery retry→failed)
├── services/classification_service.py # split resolution vs topics
├── adapters/data_lake.py              # reuse build_*_event (bias path for skipped topics)
├── tasks.py                           # stage tasks + drain
└── tests/test_close_pipeline_*.py
nexus_conversations/
├── settings.py                        # CLOSE_PIPELINE_* incl. STALE_PENDING_SECONDS, DRAIN_BATCH_SIZE, CELERY_MAX_RETRIES
└── celery.py                          # beat + routes
```

## Complexity Tracking

| Decision | Why needed |
|----------|------------|
| `ClosePipelineRecord` 1:1 vs cols on Conversation | Keeps business model lean; same 18 fields on control plane |
| Rejected: one model per stage | Avoids 4 near-identical schemas |
| Rejected: normalized stage rows in v1 | Extensibility trade-off deferred |
| Billing not gated on topics | Avoid recreating Billing lag on topics Lambda failure |
| Datalake events follow DAG | Classification after classify; topics event after topics done/skipped |
| Topics skipped still publishes | Parity with today’s bias topics event |
| Billing skipped vs failed split | Misconfig must be drain-recoverable; business skip stays intentional |
| Ops-only skipped→pending | Escape hatch without automatic re-bill of legitimate skips |
| Celery task owns pending→failed | Clear handoff vs drain; heartbeat avoids stale races |
| Billing upsert = safety net only | Stage idempotency is the real control; do not design for duplicate publishes |
| Outbox residual accepted | Honest about publish→mark crash; no sink idempotency in v1 |
| Outbox cleanup post-v1 | Avoid scope creep; acknowledge growth |
| Same-release Phase 1+2 preferred | Minimize Shape E tracking gap; gap ≠ lost Billing delivery |
| Legacy backfill inserts records | Anti-replay for drain; not a send audit |
| No DB rule `terminal ⇒ pipeline row` | Constraints static on pipeline table; Shape E stays legal |
| Shared `close_lambda` concurrency 1 | Lambda single-flight |
| `select_for_update` / unique claim | Prevent double-claim / torn stage updates |
| `{stage}_pending_at` stale clock | Only honest pending age under shape constraints |
| Drain batch size setting | Bound beat-tick work |
| Drain skips datalake waiting on topics | Avoid no-op stale requeues |
| Datalake never skipped at Shape C | Always emit path in v1; no speculative “datalake off” |
