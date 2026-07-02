# Contracts: Conversations S3 Archive

**Feature**: `001-conversations-s3-archive` | **v1.3.0**

## Standard API behavior (Phase A)

### GET `/api/v1/projects/{project_uuid}/conversations/`

Default queryset excludes expired closed conversations; in-progress always visible. **No query-param bypass.**

### GET `/api/v1/projects/{project_uuid}/conversations/{uuid}/`

`404` outside retention window. **No query-param bypass.**

---

## Support archive API (Phase D — required)

### GET `/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/`

**Auth**: Same as other conversation endpoints — `InternalTokenAuthentication` + `permissions.IsAuthenticated` (Bearer token from `INTERNAL_API_TOKENS`). No separate token or permission class.

**Behavior**: Read S3 → map to Supervisor Public V2 shape → return JSON. **Never writes Postgres.** Audit log every access (conversation UUID, project UUID, caller team, timestamp).

**Response 200** (Supervisor Public V2 compatible):

```json
{
  "conversation_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "start_date": "2026-01-15T14:30:00Z",
  "created_at": "2026-01-15T14:25:00Z",
  "ended_at": "2026-01-15T15:00:00Z",
  "status": "closed",
  "topic": "Sales",
  "channel_uuid": "660e8400-e29b-41d4-a716-446655440001",
  "contact_urn": "whatsapp:5511999999999",
  "messages": [
    {
      "text": "Hello",
      "source": "user",
      "created_at": "2026-01-15T14:30:00Z"
    }
  ],
  "archived_at": "2026-04-20T03:00:00Z",
  "is_archived": true
}
```

Field alignment: matches `SupervisorPublicConversationItemSerializer` in nexus-ai (`supervisor_public.py`) and `ConversationDetailSerializer` in nexus-conversations. Extra fields `archived_at`, `is_archived` are additive for support context.

**Response 401**: Missing or invalid Bearer token

**Response 404**: S3 object not found, or conversation still in Postgres

---

## Celery tasks (internal)

### `conversation_ms.tasks.archive_dispatcher_task`

**Trigger**: Celery Beat **every hour** (`crontab(minute=0)`)

**Queue**: `CONVERSATION_ARCHIVE_CELERY_QUEUE`

**Behavior**:
1. Acquire lock; create `ConversationArchiveBatch`
2. Select eligible conversations (timezone-aware); skip UUIDs with active/terminal records (except FAILED retry)
3. Create `ConversationArchiveRecord` (PENDING) per UUID
4. Enqueue `archive_conversation_task` with **`expires`** = now + `CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS`

### `conversation_ms.tasks.archive_conversation_task`

**Behavior**:
1. If outside optional processing window (evaluated in **project timezone**) → exit; record stays `PENDING`
2. State machine: PENDING → IN_PROGRESS → ARCHIVED → DELETED (or FAILED)
3. Idempotent S3 HEAD if object exists
4. On failure: FAILED + `errors.sentry_event_id`

---

## S3 archive object

**Key**: `{CONVERSATION_ARCHIVE_S3_PREFIX}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`

**Format**: gzip-compressed JSON, `schema_version: 1`. Full field definitions and example: [data-model.md](../data-model.md#s3-archive-document-schema_version-1).

**Integrity**: `metadata.content_sha256` must match SHA-256 of canonical uncompressed JSON (sorted keys, UTF-8) before gzip. Restore/re-read rejects unknown `schema_version`.

---

## Environment contract

| Variable | Default | Phase |
|----------|---------|-------|
| `CONVERSATION_RETENTION_DAYS` | `90` | A |
| `CONVERSATION_ARCHIVE_ENABLED` | `false` | B |
| `CONVERSATION_ARCHIVE_DRY_RUN` | `true` | B |
| `CONVERSATION_ARCHIVE_S3_BUCKET` | — | B |
| `CONVERSATION_ARCHIVE_S3_PREFIX` | `conversations-archive` | B |
| `CONVERSATION_ARCHIVE_S3_REGION` | falls back to `AWS_REGION` | B |
| `CONVERSATION_ARCHIVE_LOCK_ENABLED` | `true` | B |
| `CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS` | `7200` | B |
| `CONVERSATION_ARCHIVE_BATCH_SIZE` | `500` | B |
| `CONVERSATION_ARCHIVE_CELERY_QUEUE` | `conversations-archive` | B |
| `CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS` | `3600` | B |
| `CONVERSATION_ARCHIVE_WINDOW_START_HOUR` | — | B (optional, project TZ) |
| `CONVERSATION_ARCHIVE_WINDOW_END_HOUR` | — | B (optional, project TZ) |

Support archive API auth uses existing `INTERNAL_API_TOKENS` (no additional env var).

---

## Infrastructure contract (Argo CD)

Dedicated worker deployment consuming **only** `conversations-archive` queue.

**Reference**: `chats-engine-celery-archive`

**Owner**: **Cloud** (time de infra)

**Suggested name**: `nexus-conversations-celery-archive`

---

## Scope boundary

nexus-conversations **backend only**. No frontend. No Postgres restore. No nexus-ai / legacy DB changes.
