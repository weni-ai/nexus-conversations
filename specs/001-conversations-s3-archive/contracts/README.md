# Contracts: Conversations S3 Archive

**Feature**: `001-conversations-s3-archive` | **Date**: 2026-07-01 (updated v1.1)

## Standard API behavior (Phase A)

### GET `/api/v1/projects/{project_uuid}/conversations/`

**Change**: Default queryset excludes conversations where eligibility timestamp is older than `CONVERSATION_RETENTION_DAYS` (default 90), except in-progress (`resolution=2`).

**Unchanged**: Pagination, filters (`ConversationFilter`), `total_count`, `status_summary` — all respect retention window.

**Explicit constraint**: This endpoint MUST NOT accept query params to bypass retention (no `include_archived`, no `all_history`, etc.).

**Response shape**: No schema change.

### GET `/api/v1/projects/{project_uuid}/conversations/{uuid}/`

**Change**: Returns `404` for conversations outside retention window (same rule as list).

**Explicit constraint**: No query-param bypass. Archived-only data is accessed exclusively via the archived-conversations endpoints below.

---

## Archived conversations API (Phase D — required)

Dedicated route namespace — separate view, separate permission class, separate audit log.

### GET `/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/`

**Auth**: Internal token with support/archive scope (distinct from standard conversation read).

**Behavior**: Reads S3 only; does not query Postgres for conversation body (row may be deleted).

**Query params**:

| Param | Default | Description |
|-------|---------|-------------|
| `include_payload` | `false` | When `true`, returns full decompressed archive JSON in response |

**Response 200** (metadata only, `include_payload=false`):

```json
{
  "conversation_uuid": "uuid",
  "project_uuid": "uuid",
  "archived_at": "ISO8601",
  "s3_key": "conversations-archive/{project}/{yyyy}/{mm}/{uuid}.json.gz",
  "schema_version": 1,
  "message_count": 42,
  "eligibility_timestamp": "ISO8601"
}
```

**Response 200** (`include_payload=true`): above fields plus `"payload": { ... full archive document ... }`

**Response 403**: Token lacks support/archive scope

**Response 404**: No S3 object at deterministic key, or conversation still in Postgres (not archived)

**Safety rules**:
- MUST NOT register these routes on `ConversationViewSet`
- MUST NOT reuse list/detail serializers without explicit archive schema
- All access MUST be logged with `conversation_uuid`, `project_uuid`, caller identity

---

## Archive payload contract (S3 object)

**Content-Type**: `application/gzip`

**Key pattern**:

```text
{CONVERSATION_ARCHIVE_S3_PREFIX}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz
```

**JSON schema (uncompressed)** — see [data-model.md](../data-model.md).

**Versioning**: `schema_version` integer; restore command rejects unknown versions.

**Integrity**: `metadata.content_sha256` must match SHA-256 of canonical JSON (sorted keys, UTF-8) before gzip.

---

## Celery tasks (internal)

### `conversation_ms.tasks.archive_expired_conversations_task`

**Trigger**: Celery Beat daily 03:00 UTC

**Input kwargs**: None (reads settings)

**Return payload**:

```json
{
  "status": "success" | "skipped" | "failed",
  "reason": "string optional",
  "uploaded": 0,
  "deleted": 0,
  "failed": 0,
  "dry_run": true
}
```

**Skip reasons**: `archive_already_running` (lock held), `archive_disabled` (master switch off)

---

## Management command (Phase C)

### `python manage.py restore_conversation_from_archive`

| Argument | Required | Description |
|----------|----------|-------------|
| `--conversation-uuid` | yes | UUID to restore |
| `--project-uuid` | yes | Project scope for S3 key |
| `--dry-run` | no | Validate S3 payload without DB write |
| `--force` | no | Overwrite existing Postgres row (dangerous) |

**Exit codes**: 0 success, 1 validation error, 2 S3 not found, 3 DB conflict

**Note**: Restore re-inserts into Postgres (engineering operation). Support API consults S3 only unless a separate restore action is added later.

---

## Environment contract

| Variable | Required Phase | Default |
|----------|----------------|---------|
| `CONVERSATION_RETENTION_DAYS` | A | `90` |
| `CONVERSATION_ARCHIVE_ENABLED` | B | `false` |
| `CONVERSATION_ARCHIVE_DRY_RUN` | B | `true` |
| `CONVERSATION_ARCHIVE_S3_BUCKET` | B | — |
| `CONVERSATION_ARCHIVE_S3_PREFIX` | B | `conversations-archive` |

---

## Scope boundary

This spec covers **nexus-conversations MS only**. Legacy Nexus DB, nexus-ai V1 supervisor, and other external direct-Postgres consumers are **out of scope**.

Within MS: align `reconcile_cohort_export` and similar services to the 90-day window (see tasks T028).
