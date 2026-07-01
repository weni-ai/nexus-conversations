# Quickstart: Conversations S3 Archive

**Feature**: `001-conversations-s3-archive` | **Date**: 2026-07-01

## Prerequisites

- nexus-conversations running locally with Postgres + Redis
- AWS credentials or moto for S3 tests
- Platform: S3 bucket + IAM for staging/prod (Phase B+)

## Local configuration

Add to `.env`:

```bash
CONVERSATION_RETENTION_DAYS=90
CONVERSATION_ARCHIVE_ENABLED=true
CONVERSATION_ARCHIVE_DRY_RUN=true
CONVERSATION_ARCHIVE_S3_BUCKET=your-bucket
CONVERSATION_ARCHIVE_S3_PREFIX=conversations-archive
AWS_REGION=us-east-1
```

## Phase A — Verify API retention filter

```bash
cd nexus-conversations
poetry run pytest conversation_ms/tests/test_retention_filter.py -v
```

Manual check:

1. Create closed conversation with `end_date` 91 days ago → list API excludes it
2. Create in-progress conversation with `start_date` 100 days ago → list API includes it
3. Boundary: exactly 90 days → included (cutoff is `<`, not `<=`)

## Phase B — Dry-run archive job

```bash
poetry run python manage.py shell -c "
from conversation_ms.tasks import archive_expired_conversations_task
print(archive_expired_conversations_task.apply().result)
"
```

Verify S3:

```bash
aws s3 ls s3://your-bucket/conversations-archive/ --recursive | head
aws s3 cp s3://your-bucket/conversations-archive/{project}/{yyyy}/{mm}/{uuid}.json.gz - | gunzip | jq .
```

Confirm Postgres row count unchanged (`DRY_RUN=true`).

## Phase C — Enable deletion (staging only)

```bash
CONVERSATION_ARCHIVE_DRY_RUN=false
```

Re-run task; confirm row deleted and S3 object remains.

## Restore archived conversation

```bash
poetry run python manage.py restore_conversation_from_archive \
  --conversation-uuid=<uuid> \
  --project-uuid=<project-uuid> \
  --dry-run

# After dry-run OK:
poetry run python manage.py restore_conversation_from_archive \
  --conversation-uuid=<uuid> \
  --project-uuid=<project-uuid>
```

Verify via detail API.

## Phase D — Support archive API

```bash
# Metadata only (requires support-scoped internal token)
curl -H "Authorization: Token <support-token>" \
  "http://localhost:8000/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/"

# Full payload
curl -H "Authorization: Token <support-token>" \
  "http://localhost:8000/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/?include_payload=true"
```

```bash
poetry run pytest conversation_ms/tests/test_archived_conversations_api.py -v
```

## Test suite

```bash
poetry run pytest conversation_ms/tests/test_archive_runner.py \
  conversation_ms/tests/test_archive_payload.py \
  conversation_ms/tests/test_retention_filter.py \
  conversation_ms/tests/test_restore_archive.py \
  conversation_ms/tests/test_archived_conversations_api.py -v
```

## Staging rollout checklist

- [ ] Phase A deployed; UI shows ≤90 days
- [ ] S3 bucket + IAM verified
- [ ] Dry-run 7 days; sample payloads validated
- [ ] Metrics dashboard live
- [ ] Restore runbook tested on one sample
- [ ] Product rollout comms scheduled
- [ ] `DRY_RUN=false` enabled
- [ ] Support archive API (`archived-conversations` endpoints) deployed and tested

## Troubleshooting

| Symptom | Check |
|---------|--------|
| Task status `skipped` | Redis lock held; another worker running |
| Upload failures | IAM `s3:PutObject`; bucket region |
| Delete without upload | Must never happen — check verify step logs |
| 404 on old conversation after Phase A | Expected — use archived-conversations API or restore |
