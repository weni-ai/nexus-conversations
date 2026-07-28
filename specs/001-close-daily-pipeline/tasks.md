# Tasks: Close-Daily Four-Stage Pipeline

**Input**: [spec.md](./spec.md), [plan.md](./plan.md), [data-model.md](./data-model.md)

**Prerequisites**: Speckit tooling merged or available on parent branch (`feat/add-speckit-tooling`)

**Organization**: Mapped **1:1 to Jira Tasks / Graphite PRs** (not micro-tasks). Do not expand into one Jira issue per file.

**Tests**: Required for each PR (constraints/state machine; stage tasks; drain).

## Format

- **[Jira]** — implementation ticket
- **[US#]** — primary user story from spec.md

---

## Phase 0 — Speckit docs (this PR)

- [x] T001 Create `specs/001-close-daily-pipeline/` artifacts (spec, plan, research, data-model, quickstart, tasks, checklist)
- [x] T002 Point `.specify/feature.json` at `specs/001-close-daily-pipeline`

---

## Phase 1 — Foundation — [NEXUS-5773](https://vtex-dev.atlassian.net/browse/NEXUS-5773) [US1]

**Branch suggestion**: `feat/close-pipeline-stages-model`
**Depends on**: Speckit tooling + this spec PR

- [ ] T010 Add `ClosePipelineStageStatus` constants in `conversation_ms/close_daily/constants.py`
- [ ] T011 Add 12 columns on `Conversation` in `conversation_ms/models.py` with help_text
- [ ] T012 Write migration: add columns → backfill terminal rows to all-`done` → enable CheckConstraints from data-model.md
- [ ] T013 Implement `conversation_ms/close_daily/state_machine.py` (claim, fail, commit_classify_success, mark/reclaim per stage)
- [ ] T014 Tests: `test_close_pipeline_constraints.py` and `test_close_pipeline_state_machine.py`
- [ ] T015 Confirm close-daily runtime still uses old path (no cutover in this PR)

**Acceptance**: Jira NEXUS-5773 criteria

---

## Phase 2 — Cutover — [NEXUS-5775](https://vtex-dev.atlassian.net/browse/NEXUS-5775) [US2]

**Branch suggestion**: `feat/close-pipeline-stage-tasks`
**Depends on**: NEXUS-5773

- [ ] T020 Split `ClassificationService` into public `classify_resolution` and `classify_topics`; keep facade if needed for non-close callers
- [ ] T021 Implement stage workers under `conversation_ms/close_daily/stages/`
- [ ] T022 Add Celery tasks: classify, topics, billing, datalake in `conversation_ms/tasks.py`
- [ ] T023 Refactor `close_daily/runner.py` to selector claim+enqueue only; remove ThreadPool and inline billing/datalake
- [ ] T024 Configure `close_lambda` (concurrency 1) and side-effect queues in settings + Celery routes
- [ ] T025 Wire migrate_messages enqueue appropriately on new path
- [ ] T026 Tests: `test_close_pipeline_tasks.py`; update `test_close_daily_conversations_task.py`

**Acceptance**: Jira NEXUS-5775 criteria

---

## Phase 3 — Drain & harden — [NEXUS-5774](https://vtex-dev.atlassian.net/browse/NEXUS-5774) [US3]

**Branch suggestion**: `feat/close-pipeline-drain`
**Depends on**: NEXUS-5775

- [ ] T030 Implement `conversation_ms/close_daily/drain.py` + Beat schedule
- [ ] T031 Stale `pending` reclaim settings + behavior
- [ ] T032 Structured metrics (`close_daily/metrics.py`) + Sentry tags per stage
- [ ] T033 Shorten selector lock TTL / project task time limits
- [ ] T034 Tests: `test_close_pipeline_drain.py`
- [ ] T035 On-call notes in quickstart or short ops doc for reading the 12 columns

**Acceptance**: Jira NEXUS-5774 criteria

---

## Dependency graph

```text
T001–T002 (docs)
    → T010–T015 (NEXUS-5773)
        → T020–T026 (NEXUS-5775)
            → T030–T035 (NEXUS-5774)
```

## Notes

- Do **not** use `/speckit-taskstoissues` — Jira already created.
- Do **not** gate billing on topics.
- Do **not** raise hard time limits as the primary fix.
