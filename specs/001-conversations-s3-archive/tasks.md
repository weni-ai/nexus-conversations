# Tasks: Conversations S3 Archive & 90-Day Retention

**Spec**: v1.3.0 | **Scope**: backend only

---

## Phase 1: Setup

- [ ] T001 [P] Archive env vars in `nexus_conversations/environment.py` (incl. `CONVERSATION_ARCHIVE_*` — batch size, queue, expires, lock, region, optional window hours)
- [ ] T002 [P] Settings in `nexus_conversations/settings.py`
- [ ] T003 [P] Document in `nexus-conversations/.env.example`
- [ ] T004 Create `conversation_ms/archive/` package with `constants.py`

---

## Phase 2: Foundational

- [ ] T005 Eligibility + project timezone in `conversation_ms/archive/eligibility.py` (shared by API filter and archive; requires `ConversationMessages` row for archive)
- [ ] T006 [P] Tests in `conversation_ms/tests/test_archive_eligibility.py`
- [ ] T007 [P] Metrics helpers in `conversation_ms/archive/metrics.py`
- [ ] T008 Add `ConversationArchiveBatch` + `ConversationArchiveRecord` models + migration in `conversation_ms/models.py`
- [ ] T009 Implement `ArchiveRecordStateMachine` in `conversation_ms/archive/state_machine.py` (make unreasonable states invalid)
- [ ] T010 [P] Tests invalid transitions + DB constraints in `conversation_ms/tests/test_archive_state_machine.py` and `test_archive_tracking_models.py`

---

## Phase 3: US1 — API retention filter (MVP)

- [ ] T011 [US1] Retention filter in `conversation_ms/views.py`
- [ ] T012 [US1] Aggregates use filtered queryset in `conversation_ms/views.py`
- [ ] T013 [P] [US1] Tests in `conversation_ms/tests/test_retention_filter.py`

**Checkpoint**: Phase A deployable

---

## Phase 4: US2 — Hourly archival

- [ ] T014 [P] [US2] Payload builder in `conversation_ms/archive/payload_builder.py`
- [ ] T015 [P] [US2] S3 client in `conversation_ms/archive/s3_client.py`
- [ ] T016 [US2] Dispatcher in `conversation_ms/archive/dispatcher.py` (lock, TZ, batch cap, create PENDING records, enqueue with `expires`)
- [ ] T017 [US2] Worker in `conversation_ms/archive/worker.py` (project-TZ window check, idempotent S3, state machine, sentry_event_id on failure)
- [ ] T018 [US2] Celery tasks in `conversation_ms/tasks.py` → dedicated queue
- [ ] T019 [US2] Hourly Beat in `nexus_conversations/celery.py`
- [ ] T020 [P] [US2] Request Cloud: Argo worker `nexus-conversations-celery-archive`
- [ ] T021 [P] [US2] Dispatcher tests in `conversation_ms/tests/test_archive_dispatcher.py`
- [ ] T022 [P] [US2] Worker + moto tests (dry-run, idempotency, failure) in `conversation_ms/tests/test_archive_worker.py`

---

## Phase 5: Rollout

- [ ] T023 [P] 90-day max for `reconcile_cohort_export` in `conversation_ms/services/reconcile_cohort_export.py`
- [ ] T024 [P] Request S3 bucket + IAM from **Cloud** (time de infra)

**Checkpoint**: Safe for `DRY_RUN=false`

---

## Phase 6: Polish

- [ ] T025 [P] Structured logging in `conversation_ms/archive/worker.py`
- [ ] T026 [P] Sentry tags + `sentry_event_id` persistence in worker
- [ ] T027 [P] Stale in-progress metric in dispatcher
- [ ] T028 Run test suite on modified modules; capture list API p95 baseline pre/post Phase A (SC-006)
- [ ] T029 Validate `quickstart.md` in staging

---

## Phase 7: US3 — Support archive API (required)

- [ ] T030 [US3] `ArchiveReadProjectPermission` in `conversation_ms/api/permissions.py` (Connect RBAC; support/moderator roles) + document in `contracts/README.md`
- [ ] T031 [P] [US3] `response_adapter.py` → Supervisor V2 shape
- [ ] T032 [P] [US3] Adapter tests in `conversation_ms/tests/test_archive_response_adapter.py`
- [ ] T033 [US3] `ArchivedConversationView` in `conversation_ms/views_archived.py`
- [ ] T034 [US3] Routes in `nexus_conversations/urls.py`
- [ ] T035 [P] [US3] API tests in `conversation_ms/tests/test_archived_conversations_api.py` (mock Connect authorization)
- [ ] T036 [US3] Audit logging in `views_archived.py`

**Checkpoint**: Spec complete

---

## Phase 8: Hardening (optional)

- [ ] T037 [P] Grafana dashboard spec from tracking tables (with Cloud/SRE)
- [ ] T038 [P] Glacier lifecycle policy (Cloud)

---

## Dependencies

```text
Phase 1–2 (incl. tracking models) → Phase 3 (MVP)
                                 → Phase 4 (archive) + T020 (Cloud/Argo)
                                 → Phase 5 (delete enable)
                                 → Phase 7 (US3) — sign-off
Phase 6 — polish
Phase 8 — optional
```

| Story | Depends on |
|-------|------------|
| US1 | Phase 2 |
| US2 | Phase 2 (models + state machine), T024 S3 |
| US3 | US2 (S3 archives + records), improvements Connect auth (PR #95) |

---

## Summary

| Phase | Tasks | Count |
|-------|-------|-------|
| Setup | T001–T004 | 4 |
| Foundational | T005–T010 | 6 |
| US1 | T011–T013 | 3 |
| US2 | T014–T022 | 9 |
| Rollout | T023–T024 | 2 |
| Polish | T025–T029 | 5 |
| US3 | T030–T036 | 7 |
| Hardening | T037–T038 | 2 |
| **Total** | | **38** |

**MVP**: T001–T013 | **Sign-off**: T030–T036
