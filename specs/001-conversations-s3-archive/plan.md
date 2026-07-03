# Implementation Plan: Conversations S3 Archive & 90-Day Retention

**Branch**: `001-conversations-s3-archive` | **Spec**: [spec.md](./spec.md) v1.3.0

**Scope**: nexus-conversations **backend only**.

## Summary

1. **Phase A** — API retention filter
2. **Phase B** — Hourly dispatcher + workers, **tracking tables**, dedicated queue, dry-run
3. **Phase C** — Enable deletion
4. **Phase D (required)** — Support API → Supervisor Public V2 JSON from S3
5. **Phase E (optional)** — Grafana dashboards, Glacier lifecycle (Cloud)

**References**: Livedesk/Kallil Miro; Argo `chats-engine-celery-archive`.

## Technical Context

**Stack**: Python / Django / DRF / Celery / Postgres / S3 / Redis

**Infra owner**: **Cloud** (time de infra) — S3, IAM, Argo archive workers

**Design principle**: Make unreasonable archive states invalid (data-model.md)

## Project Structure

```text
conversation_ms/
├── models.py                    # MODIFY: ConversationArchiveBatch, ConversationArchiveRecord
├── archive/
│   ├── dispatcher.py
│   ├── worker.py
│   ├── state_machine.py         # NEW: transition guards
│   ├── eligibility.py
│   ├── payload_builder.py
│   ├── s3_client.py
│   └── response_adapter.py
├── views_archived.py
└── tests/
    ├── test_archive_state_machine.py
    ├── test_archive_tracking_models.py
    └── ...
```

## Prerequisites

| Prerequisite | Owner |
|--------------|-------|
| S3 bucket + IAM | Cloud |
| Argo `nexus-conversations-celery-archive` | Cloud |
| Improvements Connect auth merged (PR #95) | Engineering (prerequisite for Phase D) |
| `PROJECTS_API_BASE_URL` configured | Platform/Security |

## Merge order

1. Phase A → 2. Phase B (incl. tracking models) → 3. Phase C delete → 4. Phase D support API → 5. Phase E optional

## Risks

| Risk | Mitigation |
|------|------------|
| Backlog | Hourly batch + scale archive workers. **Prod sizing (03/07/2026 07:06 BRT):** 748.747 archive-eligible; ~62 d enqueue floor @ 500/h — raise `CONVERSATION_ARCHIVE_BATCH_SIZE` and worker concurrency after dry-run validation ([quickstart.md](./quickstart.md)) |
| Invalid state bugs | State machine + DB constraints + tests (SC-008) |
| Stale Celery tasks | `expires` on enqueue |
| Queue contention | Dedicated queue (Cloud/Argo) |
| Phase C delete rollback | Flip `CONVERSATION_ARCHIVE_DRY_RUN=true` and `CONVERSATION_ARCHIVE_ENABLED=false`; no automatic Postgres restore (out of scope) |
