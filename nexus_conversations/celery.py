import logging
import os
import sys
from typing import Optional

from celery import Celery, schedules
from django.conf import settings

logger = logging.getLogger(__name__)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nexus_conversations.settings")

app = Celery("nexus_conversations")
app.config_from_object("django.conf:settings", namespace="CELERY")


app.conf.imports = ("conversation_ms.tasks",)
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

task_create_missing_queues = True

app.conf.event_serializer = "json"
app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["application/json"]

# Beat schedule
# Hour 0: sync only; it enqueues close_daily when done. Hours 1–23: close only.
# While sync holds a cache lock, scheduled close_daily skips (sync may run >1h).
app.conf.beat_schedule = {
    "close_daily_conversations": {
        "task": "conversation_ms.tasks.close_daily_conversations_task",
        "schedule": schedules.crontab(minute=0, hour=list(range(1, 24))),
    },
    "sync_project_timezones": {
        "task": "conversation_ms.tasks.sync_project_timezones_task",
        "schedule": schedules.crontab(hour=0, minute=0),
    },
    "reclassify_unclassified_conversations": {
        "task": "conversation_ms.tasks.reclassify_unclassified_conversations",
        "schedule": schedules.crontab(minute=0),
    },
    "flush_project_count_buffers": {
        "task": "conversation_ms.tasks.flush_project_count_buffers",
        "schedule": schedules.schedule(run_every=60),
    },
}

if "test" in sys.argv or getattr(settings, "CELERY_ALWAYS_EAGER", False):
    from celery import current_app

    def send_task(name, args: tuple = (), kwargs: Optional[dict] = None, **opts):  # pragma: needs cover
        if kwargs is None:
            kwargs = {}
        task = current_app.tasks[name]
        return task.apply(args, kwargs, **opts)

    current_app.send_task = send_task
