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

- [ ] T010 Add `ClosePipelineStageStatus` / event-kind constants in `conversation_ms/close_daily/constants.py` (incl. `dead`)
- [ ] T011 Add `ClosePipelineRecord` (OneToOne → Conversation; **22** stage fields incl. reclaim_count) + `CloseDatalakeOutbox` (UNIQUE conversation+event_kind; created_at, published_at, last_error) in models/migrations with help_text — **no** pipeline columns on `Conversation`
- [ ] T012 Write migration: create tables → backfill terminal conversations with all-`done` records → enable CheckConstraints from data-model.md (incl. `dead` shape)
- [ ] T013 Implement `conversation_ms/close_daily/state_machine.py` (claim inserts record, fail, commit_classify_success, mark/reclaim per stage; pending_at; ops `skipped→pending`; ops `dead→pending` resets reclaim_count)
- [ ] T014 Tests: `test_close_pipeline_constraints.py` and `test_close_pipeline_state_machine.py` (incl. billing skipped vs failed; Shape E = no record; dead shape)
- [ ] T015 Confirm close-daily runtime still uses old path (no cutover in this phase)

---

## Phase 2 — Cutover — [US2]

**Depends on**: Phase 1

- [ ] T020 Split `ClassificationService` into public `classify_resolution` and `classify_topics`; keep facade if needed for non-close callers
- [ ] T021 Implement stage workers under `conversation_ms/close_daily/stages/` (datalake DAG; topics skipped → bias publish; Celery retry policy + mark failed; billing empty queue URL → failed not skipped; heartbeat pending_at)
- [ ] T022 Add Celery workers: classify, topics, billing, datalake in `conversation_ms/tasks.py` (`max_retries`: classify/topics **3**, billing/datalake **5**)
- [ ] T023 Refactor `close_daily/runner.py` to selector claim+enqueue only; remove ThreadPool and inline billing/datalake
- [ ] T024 Configure `close_lambda` (concurrency 1), side-effect queues, `CLOSE_PIPELINE_*` limits in settings + Celery routes (same Conversations image; no new Argo app)
- [ ] T025 Wire migrate_messages enqueue appropriately on new path
- [ ] T026 Tests: `test_close_pipeline_tasks.py`; update `test_close_daily_conversations_task.py` (topics skipped → bias; billing misconfig → failed)

---

## Phase 3 — Drain & harden — [US3]

**Depends on**: Phase 2

- [ ] T030 Implement `conversation_ms/close_daily/drain.py` + Beat schedule every **10 min** (queries `ClosePipelineRecord`; `CLOSE_PIPELINE_DRAIN_BATCH_SIZE` default 100)
- [ ] T031 Stale `pending` reclaim via `{stage}_pending_at` + `CLOSE_PIPELINE_STALE_PENDING_SECONDS=1800`; increment `{stage}_reclaim_count` when budget-consuming; datalake must not stale-spin while topics ∉ `{done, skipped}` (incl. topics `dead`); never auto-reclaim `skipped`/`dead`; never invent Shape E records
- [ ] T031b When next reclaim would exceed `CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5` **and** stage not outage-exempt, mark stage `dead` with error and do **not** enqueue (logical dead letter)
- [ ] T031c **Billing outage mode**: locked formula (pause / min_samples=10 / open_rate=0.5 / open_abs=50 / clear_rate=0.2 / clear_ticks=2); persist `active`+`clear_streak` in Redis key `conversation_ms:close_pipeline:billing_outage` (TTL refresh 86400s); while active, drain may re-enqueue billing but MUST NOT increment `billing_reclaim_count` and MUST NOT mark billing `dead`
- [ ] T031d Management command / documented script: **bulk** `dead → pending` (reset reclaim counts) for incident recovery
- [ ] T032 Structured metrics (`close_daily/metrics.py`) + Sentry tags per stage (aggregated `dead` counts / rate, oldest pending age, outage-mode flag from Redis, datalake-blocked-by-topics-dead); alert on spike/rate not per-row flood
- [ ] T033 Shorten selector lock TTL / project task time limits
- [ ] T034 Tests: `test_close_pipeline_drain.py` (stale clock + datalake wait rule + no Shape E + reclaim→dead + outage open/clear via Redis + topics-dead partial datalake)
- [ ] T035 On-call notes in quickstart for reading `ClosePipelineRecord` + Redis outage key (incl. datalake partial-send ats + pending_at + dead + classify-dead/In Progress + billing outage + bulk reopen)

---

## Dependency graph

```text
T001–T002 (docs)
    → T010–T015 (foundation)
        → T020–T026 (cutover)
            → T030–T035 (drain + dead letter)
```

## Notes

- Do **not** gate billing (or classification-datalake) on topics.
- Do **not** send the topics datalake event before topics is `done`/`skipped`.
- Do **not** set `close_datalake_topics_at` without publishing (incl. when topics is `skipped` — publish bias).
- Do **not** mark billing `skipped` for empty/missing queue URL — use `failed`.
- Do **not** raise hard time limits as the primary fix.
- Do **not** treat Billing upsert as a normal retry strategy — stage tracking is the recovery path.
- Do **not** claim end-to-end exactly-once for datalake; unique outbox + residual window are intentional.
- Do **not** add SQS/Celery DLQ as the dead-letter mechanism — use status `dead` + reclaim budget.
- Do **not** ship drain without billing outage mode (mass-`dead` on SQS brownout).
- Do **not** auto-bias-publish datalake when topics is `dead`.
- Prefer shipping Phase 1+2 in the same release train.
