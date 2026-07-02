# Quickstart: Conversations S3 Archive

**Spec**: v1.3.0 | **Scope**: backend only

## Local configuration

```bash
CONVERSATION_RETENTION_DAYS=90
CONVERSATION_ARCHIVE_ENABLED=true
CONVERSATION_ARCHIVE_DRY_RUN=true
CONVERSATION_ARCHIVE_S3_BUCKET=your-bucket
CONVERSATION_ARCHIVE_S3_PREFIX=conversations-archive
CONVERSATION_ARCHIVE_BATCH_SIZE=500
CONVERSATION_ARCHIVE_CELERY_QUEUE=conversations-archive
CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS=3600
# Optional: CONVERSATION_ARCHIVE_WINDOW_START_HOUR=1
# Optional: CONVERSATION_ARCHIVE_WINDOW_END_HOUR=5
```

Run archive workers on dedicated queue:

```bash
poetry run celery -A nexus_conversations worker -Q conversations-archive -l info
```

## Phase A — Retention filter

```bash
poetry run pytest conversation_ms/tests/test_retention_filter.py -v
```

## Phase B — Archive (dry-run)

```bash
poetry run python manage.py shell -c "
from conversation_ms.tasks import archive_dispatcher_task
print(archive_dispatcher_task.apply().result)
"
```

Verify:
- S3 objects created
- `ConversationArchiveRecord` rows: PENDING → IN_PROGRESS → ARCHIVED
- Postgres conversation rows unchanged (`DRY_RUN=true`)

```bash
poetry run pytest conversation_ms/tests/test_archive_state_machine.py \
  conversation_ms/tests/test_archive_tracking_models.py \
  conversation_ms/tests/test_archive_dispatcher.py \
  conversation_ms/tests/test_archive_worker.py -v
```

## Phase C — Enable deletion

`CONVERSATION_ARCHIVE_DRY_RUN=false` → records reach `DELETED`, conversation rows removed.

## Phase D — Support API

```bash
curl -H "Authorization: Bearer <user-jwt-from-support-ui>" \
  "http://localhost:8000/api/v1/projects/{project_uuid}/archived-conversations/{uuid}/"
```

Requires Connect role **support** or **moderator** on the project (`PROJECTS_API_BASE_URL` must be configured).

## Staging checklist

- [ ] Phase A deployed
- [ ] Cloud: S3 + IAM + Argo archive worker
- [ ] Dry-run 7 days; tracking table states valid
- [ ] Idempotency + FAILED + sentry_event_id tested
- [ ] `DRY_RUN=false` enabled
- [ ] Support API returns Supervisor V2 shape

## Troubleshooting

| Symptom | Check |
|---------|--------|
| Worker no-ops | Processing window config; `is_in_archive_window()` |
| Stale tasks | `CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS`; queue depth |
| Invalid state error | State machine — expected for bad transitions |
| Record stuck FAILED | `errors.sentry_event_id` in DB |
