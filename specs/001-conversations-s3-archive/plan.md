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
| `INTERNAL_API_TOKENS` (same as other endpoints) | Platform/Security |

## Merge order

1. Phase A → 2. Phase B (incl. tracking models) → 3. Phase C delete → 4. Phase D support API → 5. Phase E optional

## Risks

| Risk | Mitigation |
|------|------------|
| Backlog | Hourly batch + scale archive workers |
| Invalid state bugs | State machine + DB constraints + tests (SC-008) |
| Stale Celery tasks | `expires` on enqueue |
| Queue contention | Dedicated queue (Cloud/Argo) |
