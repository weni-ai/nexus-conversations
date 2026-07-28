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

- [ ] T010 Add `ClosePipelineStageStatus` / event-kind constants in `conversation_ms/close_daily/constants.py`
- [ ] T011 Add 18 pipeline columns on `Conversation` + `CloseDatalakeOutbox` (UNIQUE conversation+event_kind; created_at, published_at, last_error) in models/migrations with help_text
- [ ] T012 Write migration: add columns/outbox → backfill terminal rows to all-`done` → enable CheckConstraints from data-model.md
- [ ] T013 Implement `conversation_ms/close_daily/state_machine.py` (claim, fail, commit_classify_success, mark/reclaim per stage; pending_at on enter/leave pending)
- [ ] T014 Tests: `test_close_pipeline_constraints.py` and `test_close_pipeline_state_machine.py`
- [ ] T015 Confirm close-daily runtime still uses old path (no cutover in this phase)

---

## Phase 2 — Cutover — [US2]

**Depends on**: Phase 1

- [ ] T020 Split `ClassificationService` into public `classify_resolution` and `classify_topics`; keep facade if needed for non-close callers
- [ ] T021 Implement stage workers under `conversation_ms/close_daily/stages/` (datalake DAG; topics skipped → still publish bias topics event; topics finish re-enqueues datalake; never set topics_at without publish)
- [ ] T022 Add Celery workers: classify, topics, billing, datalake in `conversation_ms/tasks.py`
- [ ] T023 Refactor `close_daily/runner.py` to selector claim+enqueue only; remove ThreadPool and inline billing/datalake
- [ ] T024 Configure `close_lambda` (concurrency 1) and side-effect queues in settings + Celery routes
- [ ] T025 Wire migrate_messages enqueue appropriately on new path
- [ ] T026 Tests: `test_close_pipeline_tasks.py`; update `test_close_daily_conversations_task.py` (incl. topics skipped → bias publish)

---

## Phase 3 — Drain & harden — [US3]

**Depends on**: Phase 2

- [ ] T030 Implement `conversation_ms/close_daily/drain.py` + Beat schedule
- [ ] T031 Stale `pending` reclaim via `close_{stage}_pending_at` + `CLOSE_PIPELINE_STALE_PENDING_SECONDS`; datalake must not stale-spin while topics ∉ `{done, skipped}`
- [ ] T032 Structured metrics (`close_daily/metrics.py`) + Sentry tags per stage
- [ ] T033 Shorten selector lock TTL / project task time limits
- [ ] T034 Tests: `test_close_pipeline_drain.py` (stale clock + datalake wait rule)
- [ ] T035 On-call notes in quickstart or short ops doc for reading the pipeline columns (incl. datalake partial-send ats + pending_at)

---

## Dependency graph

```text
T001–T002 (docs)
    → T010–T015 (foundation)
        → T020–T026 (cutover)
            → T030–T035 (drain)
```

## Notes

- Do **not** gate billing (or classification-datalake) on topics.
- Do **not** send the topics datalake event before topics is `done`/`skipped`.
- Do **not** set `close_datalake_topics_at` without publishing (incl. when topics is `skipped` — publish bias).
- Do **not** raise hard time limits as the primary fix.
- Do **not** treat Billing upsert as a normal retry strategy — stage tracking is the recovery path.
- Do **not** claim end-to-end exactly-once for datalake; unique outbox + residual window are intentional.
