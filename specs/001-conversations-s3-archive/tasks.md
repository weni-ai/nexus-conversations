# Tasks: Conversations S3 Archive & 90-Day Retention

**Input**: Design documents from `specs/001-conversations-s3-archive/`

**Prerequisites**: spec.md, plan.md, research.md, data-model.md, contracts/README.md, quickstart.md

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Parallelizable
- **[Story]**: User story label from spec.md

---

## Phase 1: Setup — Configuration & scaffolding

**Purpose**: Environment variables, settings, module scaffold

- [ ] T001 [P] Add archive env vars to `nexus_conversations/environment.py` per research R8
- [ ] T002 [P] Wire settings in `nexus_conversations/settings.py` (`CONVERSATION_RETENTION_DAYS`, `CONVERSATION_ARCHIVE_*`)
- [ ] T003 [P] Document new env vars in `nexus-conversations/.env.example`
- [ ] T004 Create `conversation_ms/archive/` package: `__init__.py`, `constants.py` with lock keys in `conversation_ms/archive/constants.py`

---

## Phase 2: Foundational — Eligibility logic & shared helpers

**Purpose**: Core query logic blocking all user stories

**⚠️ CRITICAL**: No user story work until this phase completes

- [ ] T005 Implement eligibility helpers (`cutoff`, `Coalesce`, in-progress exclusion) in `conversation_ms/archive/eligibility.py`
- [ ] T006 [P] Unit tests for eligibility edge cases in `conversation_ms/tests/test_archive_eligibility.py`
- [ ] T007 [P] Add stale in-progress counter helper in `conversation_ms/archive/metrics.py`

**Checkpoint**: Eligibility Q object tested for archive and API paths

---

## Phase 3: User Story 1 — 90-day conversation list (Priority: P1) 🎯 MVP

**Goal**: API list/detail exclude expired closed conversations; in-progress always visible

**Independent Test**: `test_retention_filter.py` — day 89/90/91 boundary + in-progress >90 visible

### Implementation

- [ ] T008 [US1] Apply retention filter in `ConversationViewSet.get_queryset()` in `conversation_ms/views.py`
- [ ] T009 [US1] Ensure `list()` aggregates (`total_count`, `status_summary`) use filtered queryset in `conversation_ms/views.py`
- [ ] T010 [P] [US1] API tests for list/retrieve retention in `conversation_ms/tests/test_retention_filter.py`
- [ ] T011 [P] [US1] Boundary tests (exactly 90 days, in-progress exclusion) in `conversation_ms/tests/test_retention_filter.py`

**Checkpoint**: Phase A deployable — no deletion, API-only

---

## Phase 4: User Story 2 — Daily archival (Priority: P1)

**Goal**: Daily job uploads gzip JSON to S3, verifies, deletes when dry-run off

**Independent Test**: moto S3 integration — upload verified, delete only when `DRY_RUN=false`

### Implementation

- [ ] T012 [P] [US2] Implement payload builder in `conversation_ms/archive/payload_builder.py` (schema v1, sha256)
- [ ] T013 [P] [US2] Unit tests for payload builder in `conversation_ms/tests/test_archive_payload.py`
- [ ] T014 [P] [US2] Implement S3 upload + HEAD verify wrapper in `conversation_ms/archive/s3_client.py`
- [ ] T015 [US2] Implement runner orchestration in `conversation_ms/archive/runner.py` (lock, iterator, batch loop; skip rows without `messages_data`)
- [ ] T016 [US2] Add Celery task `archive_expired_conversations_task` in `conversation_ms/tasks.py`
- [ ] T017 [US2] Register Beat schedule (03:00 UTC) in `nexus_conversations/celery.py`
- [ ] T018 [P] [US2] Integration tests with moto in `conversation_ms/tests/test_archive_runner.py`
- [ ] T019 [P] [US2] Test dry-run skips delete in `conversation_ms/tests/test_archive_runner.py`
- [ ] T020 [P] [US2] Test verify failure retains Postgres row in `conversation_ms/tests/test_archive_runner.py`

**Checkpoint**: Phase B dry-run ready for staging

---

## Phase 5: User Story 3 — On-demand restore (Priority: P2)

**Goal**: Engineering can restore archived conversation from S3 to Postgres

**Independent Test**: Archive → delete → restore → detail API returns conversation

### Implementation

- [ ] T021 [US3] Implement `restore_conversation_from_archive` management command in `conversation_ms/management/commands/restore_conversation_from_archive.py`
- [ ] T022 [P] [US3] Tests for restore idempotency and schema validation in `conversation_ms/tests/test_restore_archive.py`
- [ ] T023 [US3] Write engineering runbook section in `specs/001-conversations-s3-archive/quickstart.md` (restore procedure)

**Checkpoint**: Phase C restore path validated

---

## Phase 6: User Story 4 — Retention transparency (Priority: P2)

**Goal**: Frontend displays 90-day notice (cross-repo)

**Independent Test**: Conversations list shows i18n notice in all 4 locales

### Implementation

- [ ] T024 [US4] Create frontend ticket: add `conversations.retention.notice` to `agent-builder-webapp` locale files (`en.json`, `pt_br.json`, `es.json`, `ro.json`)
- [ ] T025 [US4] Create frontend ticket: render notice on conversations list view in agent-builder-webapp

**Note**: T024–T025 are tracked here for traceability; implementation is in agent-builder-webapp repo.

---

## Phase 7: Rollout & in-service alignment

**Purpose**: Platform prerequisites and nexus-conversations MS internal consistency (no nexus-ai / legacy DB work)

- [ ] T026 [P] Document or enforce 90-day max window for `reconcile_cohort_export` in `conversation_ms/services/reconcile_cohort_export.py`
- [ ] T027 [P] Request S3 bucket + IAM from platform (track in plan prerequisites)
- [ ] T027a Coordinate product/ops internal rollout comms for 90-day limit (FR-014; no eng-to-customer direct contact)

**Checkpoint**: Safe to enable `CONVERSATION_ARCHIVE_DRY_RUN=false` in production

---

## Phase 8: Polish & cross-cutting

- [ ] T028 [P] Structured logging fields in `conversation_ms/archive/runner.py` per research R6
- [ ] T029 [P] Sentry tags on upload/delete failures in `conversation_ms/archive/runner.py`
- [ ] T030 [P] Emit stale in-progress metric during daily scan in `conversation_ms/archive/runner.py`
- [ ] T031 Run full test suite via project test skill on modified modules
- [ ] T032 Validate quickstart.md steps in staging

---

## Phase 9: User Story 5 — Support archive API (Priority: P2, Phase D — required)

**Goal**: Dedicated internal endpoints to consult S3 archives — separate from standard list/detail; required to complete spec

**Independent Test**: Support-scoped token → `GET .../archived-conversations/{uuid}/` returns metadata; standard list never exposes archived rows; wrong scope → 403

### Implementation

- [ ] T033 [US5] Design support/archive auth scope and permission class (document in `contracts/README.md`)
- [ ] T034 [US5] Implement `ArchivedConversationView` in `conversation_ms/views_archived.py` (S3 HEAD + optional payload fetch)
- [ ] T035 [US5] Register routes in `nexus_conversations/urls.py` under `archived-conversations/` namespace
- [ ] T036 [P] [US5] API tests: metadata, include_payload, 403, 404 in `conversation_ms/tests/test_archived_conversations_api.py`
- [ ] T037 [US5] Add access audit logging for archive consult requests in `conversation_ms/views_archived.py`

**Checkpoint**: Spec complete — support can consult S3 without engineering

---

## Phase 10: Hardening (optional, Phase E)

- [ ] T038 [P] Optional migration: `archived_at` column on Conversation (future PR)

---

## Dependencies & Execution Order

### Phase dependencies

```text
Phase 1 (Setup) → Phase 2 (Foundational) → Phase 3 (US1 MVP)
                                      ↘
                                       Phase 4 (US2) → Phase 7 (Rollout) → Phase 8 (Polish)
                                      ↗
                               Phase 5 (US3 restore)
Phase 6 (US4 frontend) — parallel after Phase 3
Phase 9 (US5 support API) — after Phase 4 stable; REQUIRED before spec sign-off
Phase 10 (Phase E hardening) — optional
```

### User story dependencies

| Story | Depends on | Independent test |
|-------|------------|------------------|
| US1 | Phase 2 | API filter tests |
| US2 | Phase 2, T027 (S3 for staging) | moto integration |
| US3 | US2 (archive exists) | restore tests |
| US4 | US1 (behavior live) | frontend RTL |
| US5 | US2 (archives in S3) | dedicated archive API tests |

### Parallel opportunities

- T001–T004 (Setup) all parallel
- T012–T014 (payload, S3 client) parallel before T015 runner
- T018–T020 (US2 tests) parallel after T015
- T024–T025 (frontend) parallel with Phase 4 backend work
- T033–T036 (US5) parallel after T014 s3_client exists

---

## Implementation strategy

### MVP first (Phase A only)

1. Complete Phase 1 + 2 + 3 (T001–T011)
2. Deploy to staging/prod — **no data deletion**
3. Validate list/detail behavior and performance

### Incremental delivery

1. MVP (US1) → deploy
2. US2 dry-run → staging validation 7 days
3. US3 restore runbook → test one sample
4. Phase 7 rollout → enable delete
5. US4 frontend notice → product rollout
6. US5 support archive API → **required before spec sign-off**
7. Phase E hardening (optional)

---

## Task summary

| Phase | Task IDs | Story | Count |
|-------|----------|-------|-------|
| Setup | T001–T004 | — | 4 |
| Foundational | T005–T007 | — | 3 |
| US1 | T008–T011 | P1 | 4 |
| US2 | T012–T020 | P1 | 9 |
| US3 | T021–T023 | P2 | 3 |
| US4 | T024–T025 | P2 | 2 |
| Rollout | T026–T027, T027a | — | 3 |
| Polish | T028–T032 | — | 5 |
| US5 (required) | T033–T037 | P2 | 5 |
| Hardening (optional) | T038 | — | 1 |
| **Total** | | | **38** |

**Suggested MVP scope**: T001–T011 (Phase A, 11 tasks)

**Parallelizable tasks**: 18 marked [P]
