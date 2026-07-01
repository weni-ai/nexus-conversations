# Data Model: Conversations S3 Archive

**Feature**: `001-conversations-s3-archive` | **Date**: 2026-07-01

## Postgres entities (existing — no v1 schema change)

### Conversation

| Field | Type | Notes |
|-------|------|-------|
| uuid | UUID PK | Archive key component |
| project_id | FK → Project | Archive path component |
| start_date, end_date | DateTime | Eligibility: prefer end_date |
| resolution | Char | `"2"` = In Progress — excluded from archive and API hide |
| contact_urn, contact_name, channel_uuid, external_id, ticket_uuid | various | Serialized in archive |
| nps, csat, has_chats_room | various | Serialized in archive |
| created_at | DateTime | Eligibility fallback |

**Table**: `intelligences_conversation`

### ConversationMessages

| Field | Type | Notes |
|-------|------|-------|
| conversation_id | OneToOne PK → Conversation | CASCADE delete on archive |
| messages | JSONField | Array of formatted message dicts |
| created_at, updated_at | DateTime | Serialized in metadata optional |

**Table**: `intelligences_conversationmessages`

### ConversationClassification

| Field | Type | Notes |
|-------|------|-------|
| uuid | UUID PK | |
| conversation_id | OneToOne → Conversation | CASCADE delete |
| topic_id, subtopic_id | FK nullable | Denormalized names in archive |
| confidence | Float | |
| created_at, updated_at | DateTime | |

**Table**: `intelligences_conversationclassification`

## Optional Phase D entity

### Conversation.archived_at (future migration)

| Field | Type | Notes |
|-------|------|-------|
| archived_at | DateTime null | Set after successful S3 upload; enables audit queries pre-delete |

**Not in v1** — constitution simplicity gate.

## S3 archive document (schema_version 1)

Logical entity stored as gzip JSON at:

`{prefix}/{project_uuid}/{yyyy}/{mm}/{conversation_uuid}.json.gz`

where `{yyyy}/{mm}` derive from eligibility timestamp (UTC).

### Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| schema_version | integer | yes | Currently `1` |
| archived_at | ISO8601 string | yes | UTC timestamp of archival |
| conversation | object | yes | Flat dict of Conversation scalar fields + project_uuid |
| messages | array | yes | May be empty array if no messages row |
| classification | object \| null | yes | topic name, subtopic name, confidence |
| metadata | object | yes | source_service, retention_days, content_sha256 |

### conversation object (serialized)

Includes: `uuid`, `project_uuid`, `contact_urn`, `contact_name`, `ticket_uuid`, `external_id`, `start_date`, `end_date`, `resolution`, `channel_uuid`, `nps`, `csat`, `has_chats_room`, `created_at`.

Dates as ISO8601 strings. UUIDs as strings.

### classification object

| Field | Type |
|-------|------|
| topic | string \| null |
| subtopic | string \| null |
| confidence | number |
| topic_uuid | string \| null |
| subtopic_uuid | string \| null |

### metadata object

| Field | Type |
|-------|------|
| source_service | `"nexus-conversations"` |
| retention_days | integer |
| content_sha256 | string (hex SHA-256 of uncompressed JSON before gzip) |

## Eligibility rules (query logic)

```python
cutoff = now - RETENTION_DAYS
eligible_ts = Coalesce(end_date, start_date, created_at)

# Archive queryset
archive_q = eligible_ts < cutoff AND resolution != IN_PROGRESS AND messages_data exists

# API retention filter (hide from default views)
hide_q = eligible_ts < cutoff AND resolution != IN_PROGRESS
```

## State transitions

```text
[Closed conversation in Postgres]
        │
        ▼ (daily archive job, age > 90d)
[Upload to S3 + verify]
        │
        ├── dry_run=true ──► [Postgres unchanged]
        │
        └── dry_run=false ──► [Delete Postgres rows] ──► [S3 only]
                                        │
                                        ▼ (manual restore command)
                                [Re-insert Postgres] ──► [Visible in UI]
```

## Deletion semantics

`Conversation.delete()` CASCADE removes:

- `ConversationMessages` (OneToOne CASCADE)
- `ConversationClassification` (OneToOne CASCADE)

No soft-delete in v1.

## Index considerations (v1)

No new indexes. Existing index on `(project, contact_urn, start_date, end_date, channel_uuid)` remains.

Monitor daily archive query cost; add partial index on `(project, end_date)` only if EXPLAIN shows seq scan at scale (deferred).
