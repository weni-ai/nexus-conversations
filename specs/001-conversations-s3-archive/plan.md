# Implementation Plan: Conversations S3 Archive & 90-Day Retention

**Branch**: `001-conversations-s3-archive` | **Date**: 2026-07-01 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-conversations-s3-archive/spec.md`

## Summary

Implement 90-day retention for closed conversations in Nexus Conversations in five program phases:

1. **Phase A** — API queryset filter (no deletion): hide expired closed conversations; keep in-progress visible.
2. **Phase B** — Daily Celery archive task in dry-run: export gzip JSON to S3, verify upload, skip delete.
3. **Phase C** — Enable deletion after staging validation; deliver engineering restore runbook.
4. **Phase D (required)** — Dedicated internal support API to consult S3 archives (separate endpoints, not query-param bypass).
5. **Phase E (optional)** — `archived_at` audit column, Glacier lifecycle per platform.

**Scope boundary**: nexus-conversations MS only. Legacy Nexus DB / nexus-ai V1 supervisor is out of scope.

Mirrors Studio `rp-archiver` pattern (scan → export → S3 → delete) natively in Django/Celery, reusing `close_daily/runner.py` orchestration and `conversation_ms/adapters/aws.py` for S3.

## Technical Context

**Language/Version**: Python 3.11+ / Django / DRF / Celery

**Primary Dependencies**: boto3 (IRSA), Redis (distributed locks), pendulum, sentry-sdk, moto (tests)

**Storage**: Postgres (`intelligences_conversation`, `intelligences_conversationmessages`, `intelligences_conversationclassification`); S3 archive bucket; DynamoDB unchanged (in-progress only)

**Testing**: pytest; patterns from `test_close_daily*`, `test_aws_adapters.py`; moto for S3 mock

**Target Platform**: Kubernetes workers (Celery Beat + workers), AWS S3

**Performance Goals**: Daily archive batch processes backlog via iterator chunk_size=500; list API p95 unchanged post-filter

**Constraints**: Upload → verify → delete (constitution); dry-run mandatory before prod delete; no new expiration column in v1

**Scale/Scope**: All projects; first run may backfill large backlog — schedule 03:00 UTC (offset from `close_daily` hours 1–23)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [x] **Service boundary**: All work in `nexus-conversations` MS only; no nexus-ai / legacy Nexus DB changes.
- [x] **Data safety**: Upload + HEAD/etag verify before delete; dry-run flag.
- [x] **Batch patterns**: Mirror `close_daily/runner.py` — global Redis lock, structured logs, Sentry.
- [x] **Test coverage**: Unit + integration tests planned per testing strategy.
- [x] **Observability**: Metrics and log fields defined in research R6.
- [x] **Configuration**: Env vars for retention days, bucket, prefix, dry-run, lock TTL.
- [x] **Simplicity**: Date cutoff query; no per-row expiration column.

**Post-design re-check**: PASS — no constitution violations.

## Project Structure

### Documentation (this feature)

```text
specs/001-conversations-s3-archive/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/README.md
├── checklists/requirements.md
├── reviews/spec-analysis.md
└── tasks.md
```

### Source Code (repository root)

```text
nexus-conversations/
├── nexus_conversations/
│   ├── celery.py                          # MODIFY: add archive beat schedule
│   ├── environment.py                     # MODIFY: archive env vars
│   └── settings.py                        # MODIFY: CONVERSATION_* settings
├── conversation_ms/
│   ├── archive/
│   │   ├── __init__.py                    # NEW
│   │   ├── constants.py                   # NEW: lock keys
│   │   ├── runner.py                      # NEW: orchestration (mirror close_daily)
│   │   ├── payload_builder.py             # NEW: gzip JSON builder
│   │   └── s3_client.py                   # NEW: upload + verify wrapper
│   ├── tasks.py                           # MODIFY: archive Celery tasks
│   ├── views.py                           # MODIFY: retention filter on queryset
│   ├── views_archived.py                  # NEW: dedicated archived-conversations endpoints (Phase D)
│   ├── filters.py                         # MODIFY (if exists) or retention mixin
│   ├── management/commands/
│   │   └── restore_conversation_from_archive.py  # NEW: v1 restore script
│   └── tests/
│       ├── test_archive_runner.py         # NEW
│       ├── test_archive_payload.py        # NEW
│       ├── test_retention_filter.py       # NEW
│       ├── test_restore_archive.py        # NEW
│       └── test_archived_conversations_api.py  # NEW (Phase D)
```

**Structure Decision**: Single Django app extension under `conversation_ms/archive/` mirroring `conversation_ms/close_daily/` layout.

## Complexity Tracking

| Tension | Why needed | Alternative rejected |
|---------|-----------|----------------------|
| Separate `archive/` module vs. inline in tasks | Testability, mirrors proven `close_daily` pattern | Inline in tasks.py — harder to test and maintain |
| Per-conversation S3 objects vs. batch files | Deterministic keys, idempotent rerun, simpler restore | Monthly batch blobs — harder partial restore |
| Optional `archived_at` column (Phase E) | Audit trail | Required in v1 — YAGNI until delete path stable |

## Prerequisites

| Prerequisite | Owner | Status |
|--------------|-------|--------|
| S3 bucket + prefix for Nexus Conversations | Platform | **Open** — block Phase B deploy |
| IAM IRSA policy (`s3:PutObject`, `s3:HeadObject`, `s3:GetObject`) | Platform | **Open** |
| Product internal rollout comms | Product/Ops | Planned |
| Frontend retention notice | agent-builder-webapp | Separate ticket (US4) |
| Support internal token scope for archive API | Platform/Security | **Open** — block Phase D |

## Implementation Phases (merge order)

1. Settings + retention queryset helper + API filter (Phase A) — deploy without delete
2. `archive/runner.py` + payload builder + S3 client + Celery task + Beat (Phase B dry-run)
3. Staging payload validation + metrics dashboards
4. Enable delete (`DRY_RUN=false`) + restore management command (Phase C)
5. Phase D: dedicated support archive API (required before spec complete)
6. Phase E: optional `archived_at` migration + Glacier lifecycle (future PR)

## Testing Strategy

| Layer | Scope |
|-------|--------|
| Unit | Cutoff Q object, in-progress exclusion, S3 key path, payload schema version |
| Integration | moto S3 upload/head; batch delete transaction; lock skip behavior |
| API | List/retrieve boundary at day 90; in-progress >90 visible |
| E2E staging | Dry-run objects valid gzip JSON; row count stable until dry-run off |

## Observability

See research R6. Key metrics: `conversations_archive.uploaded_total`, `deleted_total`, `failed_total`, `stale_in_progress_total`, `batch_duration_seconds`.

## Risks

| Risk | Mitigation |
|------|------------|
| Delete before upload | Verify HEAD/etag; per-row transaction |
| Reconcile/export >90d window | Fail-fast or document max window within MS (T028) |
| Large first-run backlog | Iterator batches; off-peak schedule |
| S3 permissions delay | Phase A can ship independently |
| Archive API auth misconfiguration | Dedicated route namespace + separate permission class |

## Open Questions (platform)

1. Dedicated vs. shared S3 bucket for Nexus
2. SSE-KMS requirement for PII archives
3. S3 lifecycle (Glacier transition, legal hold duration)
4. Org-wide archive JSON schema conformance
