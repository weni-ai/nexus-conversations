# Tasks: Close-Daily Four-Stage Pipeline

**Input**: [spec.md](./spec.md), [plan.md](./plan.md), [data-model.md](./data-model.md)

**Prerequisites**: Speckit tooling available in the repository (`.specify/`)

**Organization**: Grouped by program phase / user story so each phase can ship independently.

**Tests**: Required for each phase (constraints/state machine; stage workers; drain).

## Format

- **[P]**: Can run in parallel (different files, no dependencies)
- **[US#]**: Primary user story from spec.md

---

## Phase 0 — Spec documentation

- [x] T001 Create `specs/001-close-daily-pipeline/` artifacts (spec, plan, research, data-model, quickstart, tasks, checklist)
- [x] T002 Point `.specify/feature.json` at `specs/001-close-daily-pipeline`

---

## Phase 1 — Foundation — [US1]

**Depends on**: Phase 0

- [ ] T010 Add `ClosePipelineStageStatus` constants in `conversation_ms/close_daily/constants.py`
- [ ] T011 Add 12 columns on `Conversation` in `conversation_ms/models.py` with help_text
- [ ] T012 Write migration: add columns → backfill terminal rows to all-`done` → enable CheckConstraints from data-model.md
- [ ] T013 Implement `conversation_ms/close_daily/state_machine.py` (claim, fail, commit_classify_success, mark/reclaim per stage)
- [ ] T014 Tests: `test_close_pipeline_constraints.py` and `test_close_pipeline_state_machine.py`
- [ ] T015 Confirm close-daily runtime still uses old path (no cutover in this phase)

---

## Phase 2 — Cutover — [US2]

**Depends on**: Phase 1

- [ ] T020 Split `ClassificationService` into public `classify_resolution` and `classify_topics`; keep facade if needed for non-close callers
- [ ] T021 Implement stage workers under `conversation_ms/close_daily/stages/`
- [ ] T022 Add Celery workers: classify, topics, billing, datalake in `conversation_ms/tasks.py`
- [ ] T023 Refactor `close_daily/runner.py` to selector claim+enqueue only; remove ThreadPool and inline billing/datalake
- [ ] T024 Configure `close_lambda` (concurrency 1) and side-effect queues in settings + Celery routes
- [ ] T025 Wire migrate_messages enqueue appropriately on new path
- [ ] T026 Tests: `test_close_pipeline_tasks.py`; update `test_close_daily_conversations_task.py`

---

## Phase 3 — Drain & harden — [US3]

**Depends on**: Phase 2

- [ ] T030 Implement `conversation_ms/close_daily/drain.py` + Beat schedule
- [ ] T031 Stale `pending` reclaim settings + behavior
- [ ] T032 Structured metrics (`close_daily/metrics.py`) + Sentry tags per stage
- [ ] T033 Shorten selector lock TTL / project task time limits
- [ ] T034 Tests: `test_close_pipeline_drain.py`
- [ ] T035 On-call notes in quickstart or short ops doc for reading the 12 columns

---

## Dependency graph

```text
T001–T002 (docs)
    → T010–T015 (foundation)
        → T020–T026 (cutover)
            → T030–T035 (drain)
```

## Notes

- Do **not** gate billing on topics.
- Do **not** raise hard time limits as the primary fix.
