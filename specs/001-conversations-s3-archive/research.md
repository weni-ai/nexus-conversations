# Research: Conversations S3 Archive & 90-Day Retention

**Feature**: `001-conversations-s3-archive` | **Date**: 2026-07-01

**Grounding**: `conversations-retention-scope-technical.md`, nexus-conversations codebase audit, meeting decisions (2026-06).

---

## R1 — Retention cutoff query

**Decision**: Use Django `Coalesce("end_date", "start_date", "created_at")` compared to `timezone.now() - timedelta(days=RETENTION_DAYS)`.

**API filter**: Exclude rows where `eligible_ts < cutoff` **unless** `resolution = "2"` (In Progress).

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

## R3 — Archive orchestration pattern

**Decision**: New module `conversation_ms/archive/runner.py` mirroring `close_daily/runner.py`:

1. Acquire global Redis lock (`ARCHIVE_CONVERSATIONS_LOCK_KEY`)
2. Query eligible conversations with `.iterator(chunk_size=500)`
3. Per conversation: build payload → gzip → `put_object` → `head_object` verify etag
4. If not dry-run: `conversation.delete()` (CASCADE messages + classification)
5. Release lock; emit metrics

**Schedule**: Celery Beat crontab `minute=0, hour=3` UTC (after `close_daily` window).

**Idempotency**: If S3 key exists and etag matches expected checksum, safe to delete DB row on retry.

**Alternatives considered**:
- **Reuse rp-archiver Go service** — Rejected: wrong data model, high coupling (see technical scope doc §3).
- **Per-project sub-tasks like close_daily** — Optional future optimization; v1 uses single global batch with iterator unless profiling shows need to split.

**Codebase anchor**: `conversation_ms/close_daily/runner.py`, `conversation_ms/close_daily/constants.py`.

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

## R5 — Restore mechanism (v1)

**Decision**: Django management command `restore_conversation_from_archive`:

1. `get_object` from S3 by deterministic key (or list prefix if month unknown)
2. Validate `schema_version` and `content_sha256`
3. `transaction.atomic()`: upsert Conversation, ConversationMessages, ConversationClassification
4. Idempotent: if UUID exists, abort with clear message unless `--force`

**Support API (Phase D, required)**: Dedicated internal DRF view under `archived-conversations` route namespace — reads S3 only; does not modify standard list/detail APIs.

**Alternatives considered**:
- **Product UI restore** — Out of scope per product decision.
- **Automatic S3 rehydration on 404** — Rejected: hides latency/cost; explicit restore only.
- **`include_archived` query param on list/detail** — Rejected: weaker auth boundary; dedicated endpoint is safer.

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
| `CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS` | `7200` | Lock TTL |
| `CONVERSATION_ARCHIVE_ENABLED` | `false` | Master kill switch |

Add to `nexus_conversations/environment.py` and `settings.py`.

---

## R9 — Archived conversation access API design

**Decision**: **Dedicated endpoint namespace** — never a query param on standard list/detail.

**Routes** (internal auth, support scope):

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/` | Archive metadata from S3 HEAD + payload header parse |
| GET | `/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/?include_payload=true` | Full decompressed archive JSON in response body |

Optional future: presigned URL variant if payload size exceeds API limit — document in contracts if needed.

**Auth**: `InternalTokenAuthentication` + dedicated permission class (support/archive scope). Standard conversation read tokens MUST NOT grant access.

**Rationale**:
- Separates public retention-filtered API from archive consult path
- Clearer audit trail and permission model
- Prevents accidental exposure via forgotten query-param checks

**Alternatives considered**:
- **`include_archived=true` on list/detail** — Rejected: same route + conditional auth is error-prone.
- **Support API optional / post-v1** — Rejected: required to complete spec (can merge last).

**Codebase anchor**: new `conversation_ms/views_archived.py`, register in `nexus_conversations/urls.py`.

---

## R10 — Frontend retention notice

**Decision**: Backend does not render UI copy. agent-builder-webapp conversations module adds i18n key (4 locales per VTEX Content Guide) explaining 90-day history limit.

**Backend responsibility**: Enforce filter consistently so frontend notice matches behavior.

**Suggested i18n namespace**: `conversations.retention.notice` (frontend ticket, out of nexus-conversations v1 code path except API behavior).
