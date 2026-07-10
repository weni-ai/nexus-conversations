# Research: Conversations S3 Archive & 90-Day Retention

**Feature**: `001-conversations-s3-archive` | **Date**: 2026-07-01

**Grounding**: `conversations-retention-scope-technical.md`, nexus-conversations codebase audit, meeting decisions (2026-06).

---

## R1 — Retention cutoff query

**Decision**: Use Django `Coalesce("end_date", "start_date", "created_at")` compared to `timezone.now() - timedelta(days=RETENTION_DAYS)`.

**API filter**: Exclude rows where `eligible_ts < cutoff` **unless** `resolution = "2"` (In Progress). Cutoff computed in **project timezone** (same helper as archival).

**Archive eligibility**: Same cutoff **and** `resolution != "2"` **and** must have `ConversationMessages` row (closed snapshot exists).

**Rationale**: Matches Studio pattern; in-progress stays visible per product meeting; avoids archiving without Postgres message snapshot.

**Alternatives considered**:
- **Per-row `expires_at` column** — Rejected: adds migration, index write cost, daily scan still needed for backfill; team preferred simple filter.
- **Force-close stale in-progress** — Rejected: risks losing DynamoDB messages not yet migrated.

**Codebase anchor**: `conversation_ms/views.py` → `ConversationViewSet.get_queryset()`; new helper `conversation_ms/archive/eligibility.py`.

---

## R2 — S3 storage layout and storage class

**Decision**:

- **Path**: `s3://{bucket}/{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`
- **Format**: gzip JSON, schema_version 1 (see data-model.md)
- **Storage class on upload**: S3 Standard
- **Glacier transition**: Deferred to bucket lifecycle policy (platform-owned), not app-enforced in v1

**Rationale**: Standard storage for immediate verify/restore; Glacier retrieval latency acceptable only for cold archives accessed rarely — org policy decides transition timing.

**Alternatives considered**:
- **Data Lake** — Rejected in meeting: access/permission friction for Conversations team.
- **Glacier on first upload** — Rejected for v1: complicates verify/restore latency during dry-run validation.

**Codebase anchor**: `conversation_ms/adapters/aws.py` → `get_boto3_client("s3")`.

---

## R3 — Archive orchestration pattern (Livedesk-style)

**Decision**: Two-task pattern mirroring Livedesk chat-room archiving (Kallil) and `close_daily` fan-out:

**Task 1 — `archive_dispatcher_task`** (Celery Beat, **every hour**):
1. Acquire global Redis lock (`ARCHIVE_CONVERSATIONS_LOCK_KEY`)
2. For each project (timezone from `Project.timezone`, fallback `FALLBACK_TIMEZONE`):
   - Compute 90-day cutoff in **project local context** (same principle as `close_daily`)
   - Query eligible conversations (closed, past cutoff, has `messages_data`)
   - Select up to `ARCHIVE_BATCH_SIZE` per dispatcher run (global or per-project cap — default global cap with fair project iteration)
3. Enqueue `archive_conversation_task.delay(conversation_uuid, project_uuid)` per selected row
4. Release lock; emit `enqueued`, `backlog_remaining` metrics

**Task 2 — `archive_conversation_task`** (dedicated queue, one conversation):
1. Build payload → gzip → `put_object` → `head_object` verify
2. If not dry-run: `conversation.delete()` (CASCADE)
3. Structured log per conversation

**Schedule**: `crontab(minute=0)` — every hour, 24/7. Rate spread via batch cap avoids DB overload vs single nightly drain.

**Queue**: All archive tasks use `CONVERSATION_ARCHIVE_CELERY_QUEUE` (default `conversations-archive`). Beat `options: {queue: ...}`.

**Infrastructure**: Dedicated Celery worker deployment in Argo CD (reference: `chats-engine-celery-archive`). Cloud team provisions separate worker pool.

**Idempotency**: Deterministic S3 key; if object exists and etag matches, safe to delete DB row on retry.

**Alternatives considered**:
- **Single daily job at 03:00 UTC** — Rejected: Sandro/Kallil — backlog can outpace single run.
- **Reuse rp-archiver Go service** — Rejected: wrong data model, high coupling.
- **Monolithic iterator in one task** — Rejected: no independent scaling; blocks queue.

**Codebase anchor**: `conversation_ms/close_daily/runner.py`, Livedesk Miro `uXjVGco8kDY`.

---

## R4 — Archive payload schema

**Decision**: Single JSON document per conversation:

```json
{
  "schema_version": 1,
  "archived_at": "ISO8601",
  "conversation": { "...serialized Conversation fields..." },
  "messages": [ "...MessageMigrationService shape..." ],
  "classification": { "topic", "subtopic", "confidence" } | null,
  "metadata": {
    "source_service": "nexus-conversations",
    "retention_days": 90,
    "content_sha256": "hex digest of uncompressed JSON"
  }
}
```

Messages reuse `MessageMigrationService._format_messages_for_storage()` for consistency with Postgres storage shape.

**Alternatives considered**:
- **CSV export format** — Rejected: loses nested message structure; CSV export service is for downloads not archival.

**Codebase anchor**: `conversation_ms/services/message_migration_service.py`.

---

## R5 — Archive retrieval for support (Phase D, required)

**Decision**: Dedicated internal API reads S3 and returns **Supervisor Public V2** conversation shape. **No Postgres write.**

**Response shape** (aligned with `SupervisorPublicConversationItemSerializer` / nexus-ai `_transform_conversation`):

```json
{
  "conversation_uuid": "uuid",
  "start_date": "ISO8601 | null",
  "created_at": "ISO8601",
  "ended_at": "ISO8601 | null",
  "status": "string",
  "topic": "string",
  "channel_uuid": "uuid | null",
  "contact_urn": "string",
  "messages": [
    { "text": "...", "source": "...", "created_at": "..." }
  ],
  "archived_at": "ISO8601",
  "is_archived": true
}
```

Implementation: `archive/response_adapter.py` maps S3 archive payload → V2 shape; reuse message normalization from `MessageMigrationService` storage format.

**Alternatives considered**:
- **Re-insert into Postgres** — Rejected: Sandro — org pattern is document retrieval, not DB restore.
- **Raw S3 archive JSON to support** — Rejected: support tools expect supervisor-shaped payload.
- **Product UI self-service** — Out of scope.

**Codebase anchor**: `conversation_ms/views_archived.py`, `nexus-ai/.../supervisor_public.py` `_transform_conversation`, `conversation_ms/serializers.py` `ConversationDetailSerializer`.

---

## R6 — Observability and metrics

**Decision**: Structured log fields on every archive operation:

`conversation_uuid`, `project_uuid`, `s3_key`, `dry_run`, `deleted`, `bytes_uploaded`, `duration_ms`

**Metrics** (Prometheus/Datadog compatible counters via existing patterns or statsd if available):

| Metric | Type |
|--------|------|
| `conversations_archive.uploaded_total` | counter |
| `conversations_archive.deleted_total` | counter |
| `conversations_archive.failed_total` | counter |
| `conversations_archive.stale_in_progress_total` | counter (daily scan) |
| `conversations_archive.batch_duration_seconds` | histogram |

Sentry: capture upload/delete exceptions with tags `project_uuid`, `conversation_uuid`.

---

## R7 — In-service export/reconcile alignment (nexus-conversations only)

**Decision**: Before Phase C (delete enablement), align **within nexus-conversations MS only**:

1. `reconcile_cohort_export` — document or enforce 90-day max query window; fail fast with clear error if window exceeds retention
2. `conversation_csv_export_service` — same 90-day constraint if applicable

**Out of scope**: nexus-ai, legacy Nexus DB, nexus-ai V1 supervisor direct Postgres reads — not this spec's responsibility.

**Rationale**: Consumers of the nexus-conversations HTTP API inherit retention from Phase A filter automatically. Direct-ORM callers outside this MS are explicitly excluded from scope.

**Codebase anchor**: `conversation_ms/services/reconcile_cohort_export.py`, `conversation_ms/views.py`.

---

## R8 — Configuration surface

**Decision**: New environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `CONVERSATION_RETENTION_DAYS` | `90` | Retention window |
| `CONVERSATION_ARCHIVE_S3_BUCKET` | `""` | Required for Phase B+ |
| `CONVERSATION_ARCHIVE_S3_PREFIX` | `conversations-archive` | S3 key prefix |
| `CONVERSATION_ARCHIVE_S3_REGION` | falls back to `AWS_REGION` | Region |
| `CONVERSATION_ARCHIVE_DRY_RUN` | `true` | Safety gate |
| `CONVERSATION_ARCHIVE_LOCK_ENABLED` | `true` | Distributed lock |
| `CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS` | `120` | Lock TTL (heartbeat renews while dispatcher runs) |
| `CONVERSATION_ARCHIVE_LOCK_HEARTBEAT_EVERY` | `100` | Renew lock every N enqueues |
| `CONVERSATION_ARCHIVE_LOCK_STALE_SECONDS` | `1800` | Steal lock when owner batch older than this |
| `CONVERSATION_ARCHIVE_ENABLED` | `false` | Master kill switch |
| `CONVERSATION_ARCHIVE_BATCH_SIZE` | `500` | Max conversations enqueued per hourly dispatcher run |
| `CONVERSATION_ARCHIVE_CELERY_QUEUE` | `conversations-archive` | Dedicated Celery queue name |
| `CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS` | `3600` | Celery expires on enqueued worker tasks |
| `CONVERSATION_ARCHIVE_WINDOW_START_HOUR` | `null` | Optional processing window start (**project timezone**) |
| `CONVERSATION_ARCHIVE_WINDOW_END_HOUR` | `null` | Optional processing window end (**project timezone**) |

Archive API auth (Phase D): reuses `PROJECTS_API_BASE_URL` + `PROJECT_AUTH_API_TIMEOUT_SECONDS` from improvements Connect RBAC (PR #95).

Add to `nexus_conversations/environment.py` and `settings.py`.

---

## R9 — Archived conversation access API design

**Decision**: **Dedicated endpoint** returning **Supervisor Public V2** JSON shape — never a query param on standard list/detail.

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/` | Retrieve archived conversation (V2 shape) from S3 |

**Auth**: User JWT + Connect project authorization (improvements pattern, PR #95). Permission class `ArchiveReadProjectPermission` requires Connect role **support (4) or moderator (3)** for GET.

**Behavior**: S3 `get_object` → validate → `response_adapter` → V2 JSON. No Postgres write. Audit log every access (`project_auth_user_email`).

**Alternatives considered**:
- **`include_archived` on list/detail** — Rejected.
- **Raw archive payload / `include_payload` param** — Rejected: support needs V2 shape (Sandro).
- **Postgres restore** — Rejected.

**Codebase anchor**: `conversation_ms/views_archived.py`, `conversation_ms/archive/response_adapter.py`.

---

## R10 — Dedicated Celery worker (Argo CD)

**Decision**: Provision a separate Kubernetes deployment for archive workers, consuming only `conversations-archive` queue.

**Reference**: `chats-engine-celery-archive` in Argo CD (Livedesk pattern per Sandro).

**Worker command**: `celery -A nexus_conversations worker -Q conversations-archive --concurrency=N`

**Rationale**: Archive is I/O-heavy (S3, gzip, large JSON); isolates from `close_daily`, `reclassify`, and API-triggered tasks.

**Owner**: **Cloud** (time de infra) — Argo CD manifest; engineering provides queue name + env contract.

**Alternatives considered**:
- **Shared default queue** — Rejected: Sandro — concurrency starvation risk.

---

## R11 — Archive tracking tables (make unreasonable states invalid)

**Decision**: Add `ConversationArchiveBatch` + `ConversationArchiveRecord` with strict state machine.

**Principle**: Follow **make unreasonable states invalid** — invalid states are unrepresentable (DB) or rejected (service), not caught only at runtime.

**Mechanisms**:
- Enum status with explicit transition graph in `ArchiveRecordStateMachine`
- UNIQUE(`conversation_uuid`) — one archive record per conversation
- Conditional NOT NULL: `s3_key` required when status ≥ ARCHIVED
- `errors` JSON stores `{ "message": "...", "sentry_event_id": "..." }` on FAILED
- Dispatcher skips UUIDs with record in non-retryable terminal/active states

**Reference**: Livedesk Miro `roomarchivedconversation` + status sticky note.

**Alternatives considered**:
- **`archived_at` column on Conversation** — Rejected: row deleted in Phase C; tracking table survives.
- **Logs/metrics only** — Rejected: no post-delete audit, weak fail-safe.

**Codebase anchor**: `conversation_ms/models.py`, `conversation_ms/archive/state_machine.py`.

---

## R12 — Celery expires & processing window

**Decision**:
- Dispatcher enqueues `archive_conversation_task.apply_async(..., expires=now + CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS)`
- Worker checks `is_in_archive_window()` at start in **project timezone**; if false, exit without state change (record stays `PENDING`)

**Rationale**: Livedesk Miro task 2 + quick notes; prevents stale queue backlog from hammering DB.

**Alternatives considered**:
- **Hard 1h–5h only window** — Rejected as sole strategy; Sandro prefers hourly + batch cap. Window is optional guardrail.

---

## R13 — Idempotent already-archived path

**Decision**: Before upload, HEAD S3 object. If exists and `content_sha256` matches (or etag valid), transition to ARCHIVED without re-upload. If dry-run off and Postgres row exists, proceed to delete.

**Rationale**: Livedesk fail-safe "fail archival of already archived rooms"; safe retries.

---

## R14 — Scope: backend only

**Decision**: No frontend deliverables. Retention enforcement is API behavior only. Product/UI comms out of engineering scope.

**Out of scope**: Frontend deliverables; retention enforced via API only.

---

## R15 — S3 upload/verify retry policy

**Decision**: Transient S3 errors (timeouts, 5xx) use Celery task autoretry with exponential backoff (in addition to boto3 client retries). Terminal failures transition record to `FAILED` with `sentry_event_id` (`FR-023`). **Never delete Postgres** unless upload + verify succeeded in the same worker attempt (or idempotent HEAD path per R13).

**Rationale**: `FR-006` requires verify before delete but leaves retry mechanics to implementation; explicit policy avoids ambiguous worker behavior.

---

## R16 — Production backlog baseline

**Decision**: Size backlog before Phase B/C rollout using the sizing script in [quickstart.md](./quickstart.md#backlog-sizing-production).

**Snapshot (production, 03/07/2026 07:06 BRT / 2026-07-03T10:06:45Z):**

| Metric | Count |
|--------|------:|
| archive_eligible | 748.747 |
| api_hidden_closed | 810.693 |
| stale_in_progress | 127.473 |

Enqueue floor @ 500/h: ~62 days. Mitigation: raise `CONVERSATION_ARCHIVE_BATCH_SIZE` and scale archive workers after dry-run validation — not a separate code phase.
