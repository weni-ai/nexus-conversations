# Data Model: Conversations S3 Archive

**Feature**: `001-conversations-s3-archive` | **v1.3.0**

## Design principle: make unreasonable states invalid

Archive lifecycle state is enforced at two layers:

1. **Database** — enum status, conditional NOT NULL, unique keys, CHECK constraints where Django supports them
2. **Service** — `ArchiveRecordStateMachine` rejects transitions not in the allowed graph; workers never set fields directly

Invalid examples that MUST be impossible:

| Invalid state | Prevention |
|---------------|------------|
| `DELETED` without `s3_key` | `s3_key` NOT NULL when status ∈ `{ARCHIVED, DELETED}` |
| `deleted_at` set while status ≠ `DELETED` | Service guard + constraint |
| Two active records for same `conversation_uuid` | UNIQUE on `conversation_uuid` |
| `ARCHIVED` without `archived_at` | NOT NULL when status ≥ `ARCHIVED` |
| Skip `IN_PROGRESS` → jump to `DELETED` | State machine only allows sequential transitions |
| `FAILED` → `DELETED` | Must retry through `IN_PROGRESS` |

---

## Existing Postgres entities

These tables are **not modified** by this feature. After Phase C, eligible conversation rows are **deleted** (CASCADE to related tables); archive state is tracked in `ConversationArchiveRecord`.

### Conversation

| Field | Type | Notes |
|-------|------|-------|
| uuid | UUID PK | Archive key component |
| project_id | FK → Project | Archive path component |
| start_date, end_date | DateTime | Eligibility: prefer `end_date` |
| resolution | Char | `"2"` = In Progress — excluded from archive and API retention filter |
| contact_urn, contact_name, channel_uuid, external_id, ticket_uuid | various | Serialized in S3 payload |
| nps, csat, has_chats_room | various | Serialized in S3 payload |
| created_at | DateTime | Eligibility fallback |

**Table**: `intelligences_conversation`

### ConversationMessages

| Field | Type | Notes |
|-------|------|-------|
| conversation_id | OneToOne PK → Conversation | CASCADE delete on archive |
| messages | JSONField | Array of message dicts (`text`, `source`, `created_at`; optional `message_id`/`uuid`) |
| created_at, updated_at | DateTime | |

**Table**: `intelligences_conversationmessages`

### ConversationClassification

| Field | Type | Notes |
|-------|------|-------|
| uuid | UUID PK | |
| conversation_id | OneToOne → Conversation | CASCADE delete |
| topic_id, subtopic_id | FK nullable | Denormalized names stored in S3 payload |
| confidence | Float | |
| created_at, updated_at | DateTime | |

**Table**: `intelligences_conversationclassification`

---

## New entities (Phase B)

### ConversationArchiveBatch

One row per dispatcher run (Livedesk `archiveconversationstaskinfo` equivalent).

| Field | Type | Constraints |
|-------|------|-------------|
| id | UUID PK | |
| started_at | DateTime | NOT NULL |
| finished_at | DateTime | NULL until complete |
| enqueued_count | Integer | default 0 |
| dry_run | Boolean | NOT NULL |

**Table**: `conversation_ms_conversationarchivebatch`

### ConversationArchiveRecord

One row per conversation ever processed for archival. **Survives** conversation row deletion.

| Field | Type | Constraints |
|-------|------|-------------|
| id | UUID PK | |
| conversation_uuid | UUID | UNIQUE, NOT NULL |
| project_uuid | UUID | NOT NULL, indexed |
| batch_id | FK → Batch | NOT NULL |
| status | Enum | NOT NULL — see below |
| s3_key | VARCHAR | NULL until ARCHIVED; NOT NULL when status ∈ {ARCHIVED, DELETED} |
| started_at | DateTime | NOT NULL |
| archived_at | DateTime | NULL until ARCHIVED |
| deleted_at | DateTime | NULL until DELETED |
| failed_at | DateTime | NULL until FAILED |
| finished_at | DateTime | NULL until terminal state |
| errors | JSONField | NULL; includes `sentry_event_id` on failure |
| content_sha256 | CHAR(64) | NULL until upload verified |

**Table**: `conversation_ms_conversationarchiverecord`

### Status enum & allowed transitions

```text
PENDING ──► IN_PROGRESS ──► ARCHIVED ──► DELETED   (terminal)
                │               │
                └──────► FAILED ◄┘ (retry: FAILED → IN_PROGRESS only)
```

| Status | Meaning |
|--------|---------|
| `PENDING` | Dispatcher created record; worker not started |
| `IN_PROGRESS` | Worker running |
| `ARCHIVED` | S3 upload verified; Postgres row not yet deleted (or dry-run) |
| `DELETED` | Postgres conversation removed (terminal) |
| `FAILED` | Error; `errors` contains detail + `sentry_event_id` |

**Dry-run**: terminal state is `ARCHIVED` (never `DELETED`).

---

## S3 archive document (schema_version 1)

**Path**: `{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`

where `{yyyy}/{mm}` derive from the eligibility timestamp normalized to **UTC** (deterministic S3 keys regardless of project timezone).

**Content-Type on wire**: `application/gzip` (JSON below is the uncompressed document).

### Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| schema_version | integer | yes | Currently `1` |
| archived_at | ISO8601 string | yes | UTC timestamp of archival |
| conversation | object | yes | Flat dict of Conversation scalar fields + `project_uuid` |
| messages | array | yes | May be empty array if no messages row |
| classification | object \| null | yes | Topic/subtopic/confidence |
| metadata | object | yes | Integrity and source metadata |

### `conversation` object

Includes: `uuid`, `project_uuid`, `contact_urn`, `contact_name`, `ticket_uuid`, `external_id`, `start_date`, `end_date`, `resolution`, `channel_uuid`, `nps`, `csat`, `has_chats_room`, `created_at`.

Dates as ISO8601 strings. UUIDs as strings.

### `messages` array items

Each item matches Postgres storage shape (from `MessageMigrationService._format_messages_for_storage`):

| Field | Type | Required |
|-------|------|----------|
| text | string | yes |
| source | string | yes |
| created_at | string | yes |
| message_id | string | optional |
| uuid | string | optional (same as `message_id` when present) |

No media fields in `schema_version` 1 (current message schema has no attachments).

### `classification` object

| Field | Type |
|-------|------|
| topic | string \| null |
| subtopic | string \| null |
| confidence | number |
| topic_uuid | string \| null |
| subtopic_uuid | string \| null |

### `metadata` object

| Field | Type |
|-------|------|
| source_service | `"nexus-conversations"` |
| retention_days | integer |
| content_sha256 | string (hex SHA-256 of canonical uncompressed JSON before gzip) |

### Example (uncompressed)

```json
{
  "schema_version": 1,
  "archived_at": "2026-04-20T03:00:00Z",
  "conversation": {
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "project_uuid": "660e8400-e29b-41d4-a716-446655440001",
    "contact_urn": "whatsapp:5511999999999",
    "contact_name": "Customer",
    "start_date": "2026-01-15T14:25:00Z",
    "end_date": "2026-01-15T15:00:00Z",
    "resolution": "0",
    "channel_uuid": "770e8400-e29b-41d4-a716-446655440002",
    "created_at": "2026-01-15T14:25:00Z"
  },
  "messages": [
    {
      "text": "Hello",
      "source": "user",
      "created_at": "2026-01-15T14:30:00Z"
    }
  ],
  "classification": {
    "topic": "Sales",
    "subtopic": "Pricing",
    "confidence": 0.92,
    "topic_uuid": null,
    "subtopic_uuid": null
  },
  "metadata": {
    "source_service": "nexus-conversations",
    "retention_days": 90,
    "content_sha256": "abc123..."
  }
}
```

---

## Eligibility rules

Cutoff per project timezone (`Project.timezone`, fallback `FALLBACK_TIMEZONE`), consistent with `close_daily`:

```python
eligible_ts = Coalesce(end_date, start_date, created_at)
archive_q = eligible_ts < cutoff AND resolution != IN_PROGRESS AND messages_data exists
```

Dispatcher excludes conversations that already have a record in `{IN_PROGRESS, ARCHIVED, DELETED}` unless `FAILED` (retry eligible).

---

## State transitions (end-to-end)

```text
[Conversation in Postgres]
        │
        ▼ dispatcher creates record (PENDING) → worker (IN_PROGRESS)
[Upload S3 + verify] → record ARCHIVED
        │
        ├── dry_run ──► stop (Postgres unchanged)
        │
        └── not dry_run ──► delete Conversation → record DELETED

[support API] reads S3 via record.s3_key → Supervisor V2 JSON (no Postgres)
```

---

## Index considerations

- `ConversationArchiveRecord`: index on `(status, project_uuid)` for backlog/dashboard queries
- `ConversationArchiveRecord`: index on `(batch_id)`
- Monitor eligibility query cost on `intelligences_conversation`; partial index deferred

---

## Observability (Grafana)

Dashboards sourced from tracking tables (Phase E):

- Records by status over time
- FAILED count + recent `sentry_event_id`
- Dispatcher `enqueued_count` vs backlog
- Time from `PENDING` → `DELETED`
