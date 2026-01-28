import logging
import os
import sys
from typing import Optional

from celery import Celery
from django.conf import settings

logger = logging.getLogger(__name__)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nexus_conversations.settings")

app = Celery("nexus_conversations")
app.config_from_object("django.conf:settings", namespace="CELERY")

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

task_create_missing_queues = True

app.conf.event_serializer = "json"
app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["application/json"]

# Beat schedule - empty for now, will be populated when timezone-based closing is implemented
app.conf.beat_schedule = {
    # Future tasks will be added here
    # Example:
    # "close_conversations": {
    #     "task": "conversation_ms.tasks.close_conversations",
    #     "schedule": schedules.crontab(hour=0, minute=0),
    # },
}

if "test" in sys.argv or getattr(settings, "CELERY_ALWAYS_EAGER", False):
    from celery import current_app

    def send_task(name, args: tuple = (), kwargs: Optional[dict] = None, **opts):  # pragma: needs cover
        if kwargs is None:
            kwargs = {}
        task = current_app.tasks[name]
        return task.apply(args, kwargs, **opts)

    current_app.send_task = send_task
