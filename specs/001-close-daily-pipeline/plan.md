# Implementation Plan: Close-Daily Four-Stage Pipeline

**Branch**: `001-close-daily-pipeline` | **Date**: 2026-07-28 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-close-daily-pipeline/spec.md`

## Summary

Replace the monolithic per-project close-daily batch (classify + topics + billing + datalake in one long Celery task with ThreadPool Lambda fan-out) with a **four-stage pipeline** on `Conversation`: classify, topics, billing, datalake. Each stage has `status` + `*_at` + `*_error`. Mutations go only through `ClosePipelineStateMachine`. DB `CheckConstraint`s make illegal combinations unrepresentable. Classify and topics run on a concurrency-1 Lambda queue; billing and datalake are independent after classify and do not wait on topics. Drain recovers `failed` / stale `pending`. Legacy terminal rows are backfilled as all-`done` (*legacy assumed complete*).

## Technical Context

**Language/Version**: Python 3.11 (project Poetry)

**Primary Dependencies**: Django, Celery, Redis (locks/cache), boto3 (Lambda), existing SQS billing producer, existing datalake Celery events

**Storage**: PostgreSQL (`Conversation` / `intelligences_conversation`)

**Testing**: pytest / Django TestCase patterns in `conversation_ms/tests/`

**Target Platform**: nexus-conversations MS (Celery workers + Beat)

**Project Type**: backend microservice

**Performance Goals**: Correctness and resumability over wall-clock speed; serial Lambda accepted

**Constraints**: Resolution Lambda single-flight; no Billing reconcile; no raise hard time limit as primary fix

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
├── models.py                          # 12 columns + CheckConstraints
├── migrations/00xx_close_pipeline_*.py
├── close_daily/
│   ├── constants.py                   # stage enum, queue names
│   ├── state_machine.py               # NEW
│   ├── metrics.py                     # NEW
│   ├── drain.py                       # NEW (Phase 3)
│   ├── runner.py                      # selector-only after cutover
│   └── stages/                        # NEW workers (Phase 2)
├── services/classification_service.py # split resolution vs topics
├── tasks.py                           # stage tasks + drain
└── tests/test_close_pipeline_*.py
nexus_conversations/
├── settings.py                        # CLOSE_PIPELINE_*
└── celery.py                          # beat + routes
```

## Complexity Tracking

| Decision | Why needed |
|----------|------------|
| 12 columns vs JSON blob | Enables CheckConstraints, drain indexes, clear ops queries |
| Billing not gated on topics | Avoid recreating Billing lag on topics Lambda failure |
| Legacy backfill as `done` | Satisfies strong constraints / anti-replay; not a send audit |
| Shared `close_lambda` queue concurrency 1 | Hard Lambda single-flight constraint |
