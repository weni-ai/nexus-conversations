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

## Backlog sizing (production)

Measurement run in **production** Postgres via `manage.py shell` on an Argo API pod.

| Field | Value |
|-------|-------|
| **Measured at (UTC)** | 2026-07-03T10:06:45Z |
| **Measured at (BRT)** | 03/07/2026 07:06:45 |
| **Retention days** | 90 |
| **Batch size (default)** | 500 / dispatcher hour |

| Metric | Count | Notes |
|--------|------:|-------|
| `archive_eligible` | **748.747** | Closed, `messages_data` exists, past cutoff — Phase B backfill |
| `api_hidden_closed` | 810.693 | Phase A list/detail filter (closed past cutoff) |
| `stale_in_progress` | 127.473 | In-progress past cutoff — visible, not archived |
| Gap (API − archive) | ~62.946 | Closed without `ConversationMessages` snapshot |

**Enqueue ETA** (conservative: 500 enqueued/hour, serial processing): **~1.497 h (~62,4 days)**.

Parallel archive workers drain the queue faster than the enqueue rate; treat 62 days as a **floor**, not wall-clock time.

### Top projects (archive_eligible)

| Count | Project UUID |
|------:|--------------|
| 230.282 | `385c8443-249e-462e-a287-f4a0dc292915` |
| 183.624 | `76396786-80de-4dd1-b65a-31bf006435cc` |
| 70.950 | `19f91ba7-185b-4d13-916f-b2f38e887dd2` |
| 38.541 | `37e1e32b-1460-4278-be9e-824ee821bcbe` |
| 29.908 | `52a1bb29-2602-4f5a-988a-0fed9941c3c0` |

Top 2 projects ≈ **55%** of backlog — tune batch/workers with this skew in mind.

### Tuning (operational — no separate backfill phase)

| Lever | Example | Effect |
|-------|---------|--------|
| `CONVERSATION_ARCHIVE_BATCH_SIZE` | 500 → 2000 | ~4× enqueue rate (~15,5 days floor) |
| Archive worker replicas | 1 → 4 | Higher parallel S3 upload throughput |
| Celery `--concurrency` | scale per pod | More tasks in flight on `conversations-archive` queue |

Run **Phase B dry-run** in staging with raised batch size; watch DB p95 and S3 error rate before prod.

### Re-run sizing (`exec` in `manage.py shell`)

Paste once in the pod shell (per-project timezone cutoff, same rules as spec):

```python
exec("""
from django.conf import settings
from django.db.models import Q
from django.db.models.functions import Coalesce
import pendulum
from conversation_ms.models import Conversation, Project
from conversation_ms.utils.date_helpers import resolve_effective_project_timezone

RETENTION_DAYS = int(getattr(settings, "CONVERSATION_RETENTION_DAYS", 90))
IN_PROGRESS = "2"
BATCH_SIZE = int(getattr(settings, "CONVERSATION_ARCHIVE_BATCH_SIZE", 500))
now = pendulum.now("UTC")

def cutoff_utc(project_timezone):
    tz = resolve_effective_project_timezone(project_timezone)
    return now.in_timezone(tz).start_of("day").subtract(days=RETENTION_DAYS).in_timezone("UTC")

total_archive_eligible = total_api_hidden_closed = total_stale_in_progress = 0
by_project = []
for project in Project.objects.only("uuid", "name", "timezone").iterator():
    cutoff = cutoff_utc(project.timezone)
    base = Conversation.objects.filter(project_id=project.uuid).annotate(
        eligible_ts=Coalesce("end_date", "start_date", "created_at"),
    )
    archive_n = base.filter(~Q(resolution=IN_PROGRESS), eligible_ts__lt=cutoff, messages_data__isnull=False).count()
    api_hidden_n = base.filter(~Q(resolution=IN_PROGRESS), eligible_ts__lt=cutoff).count()
    stale_ip_n = base.filter(resolution=IN_PROGRESS, eligible_ts__lt=cutoff).count()
    total_archive_eligible += archive_n
    total_api_hidden_closed += api_hidden_n
    total_stale_in_progress += stale_ip_n
    if archive_n:
        by_project.append((archive_n, str(project.uuid), project.name or ""))

by_project.sort(reverse=True)
hours_to_drain = total_archive_eligible / BATCH_SIZE if BATCH_SIZE else 0
print(f"as_of_utc: {now.isoformat()}")
print(f"archive_eligible: {total_archive_eligible:,}")
print(f"api_hidden_closed: {total_api_hidden_closed:,}")
print(f"stale_in_progress: {total_stale_in_progress:,}")
print(f"eta_enqueue_hours: {hours_to_drain:,.0f} (~{hours_to_drain/24:,.1f} days @ {BATCH_SIZE}/h)")
for n, uuid, name in by_project[:10]:
    print(f"  {n:>8,}  {uuid}  {name}")
""")
```

## Troubleshooting

| Symptom | Check |
|---------|--------|
| Worker no-ops | Processing window config; `is_in_archive_window()` |
| Stale tasks | `CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS`; queue depth |
| Invalid state error | State machine — expected for bad transitions |
| Record stuck FAILED | `errors.sentry_event_id` in DB |
