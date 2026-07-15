"""Per-conversation archive worker orchestration (no Celery decorators)."""

from __future__ import annotations

import logging
import time
from typing import Any
from uuid import UUID

import pendulum
import sentry_sdk
from django.conf import settings

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.archive.metrics import log_archive_event
from conversation_ms.archive.payload_builder import build_archive_artifact
from conversation_ms.archive.s3_client import ArchiveS3Client, TransientS3Error
from conversation_ms.archive.state_machine import ArchiveRecordStateMachine, InvalidArchiveStateTransition
from conversation_ms.models import Conversation, ConversationArchiveRecord, Project
from conversation_ms.utils.date_helpers import resolve_effective_project_timezone

logger = logging.getLogger(__name__)


def is_in_archive_window(
    project_timezone: str | None,
    *,
    now: pendulum.DateTime | None = None,
) -> bool:
    start_hour = getattr(settings, "CONVERSATION_ARCHIVE_WINDOW_START_HOUR", None)
    end_hour = getattr(settings, "CONVERSATION_ARCHIVE_WINDOW_END_HOUR", None)
    if start_hour is None or end_hour is None:
        return True

    effective_tz = resolve_effective_project_timezone(project_timezone)
    local_hour = (now or pendulum.now("UTC")).in_timezone(effective_tz).hour
    if start_hour <= end_hour:
        return start_hour <= local_hour < end_hour
    return local_hour >= start_hour or local_hour < end_hour


def _archive_dry_run() -> bool:
    return bool(getattr(settings, "CONVERSATION_ARCHIVE_DRY_RUN", True))


def _begin_processing(record: ConversationArchiveRecord) -> ConversationArchiveRecord:
    if record.status == ArchiveRecordStatus.PENDING:
        return ArchiveRecordStateMachine.transition_to_in_progress(record)
    if record.status == ArchiveRecordStatus.FAILED:
        return ArchiveRecordStateMachine.transition_to_in_progress(record)
    if record.status == ArchiveRecordStatus.IN_PROGRESS:
        return record
    raise InvalidArchiveStateTransition(f"Cannot start archive worker for record in status {record.status!r}")


def process_archive_conversation(record_id: str | UUID) -> dict[str, Any]:
    """
    Archive one conversation: optional window guard, S3 upload/verify, state transitions.

    With ``CONVERSATION_ARCHIVE_DRY_RUN=true`` Postgres rows are preserved after ARCHIVED.
    """
    started = time.monotonic()
    record = ConversationArchiveRecord.objects.get(pk=record_id)
    project = Project.objects.filter(uuid=record.project_uuid).only("timezone").first()
    project_timezone = project.timezone if project else None

    if not is_in_archive_window(project_timezone):
        log_archive_event(
            "worker_skipped_window",
            conversation_uuid=record.conversation_uuid,
            project_uuid=record.project_uuid,
            record_id=record.id,
        )
        return {"status": "skipped", "reason": "outside_processing_window"}

    if record.status in {ArchiveRecordStatus.ARCHIVED, ArchiveRecordStatus.DELETED}:
        return {"status": "skipped", "reason": "already_terminal", "status_value": record.status}

    try:
        conversation = Conversation.objects.select_related("project").get(uuid=record.conversation_uuid)
    except Conversation.DoesNotExist:
        if record.status == ArchiveRecordStatus.ARCHIVED:
            return {"status": "skipped", "reason": "conversation_already_removed"}
        raise

    dry_run = _archive_dry_run()
    record = _begin_processing(record)
    s3 = ArchiveS3Client()

    try:
        _payload, gz_body, content_sha256, s3_key = build_archive_artifact(conversation)

        existing_sha = s3.get_valid_existing_archive(s3_key, str(conversation.uuid))
        if existing_sha:
            content_sha256 = existing_sha
            log_archive_event(
                "worker_s3_idempotent_hit",
                conversation_uuid=record.conversation_uuid,
                s3_key=s3_key,
            )
        else:
            s3.put_gzip_object(s3_key, gz_body)
            s3.verify_object_exists(s3_key)

        record = ArchiveRecordStateMachine.transition_to_archived(
            record,
            s3_key=s3_key,
            content_sha256=content_sha256,
            dry_run=dry_run,
        )

        deleted = False
        if not dry_run:
            conversation.delete()
            record = ArchiveRecordStateMachine.transition_to_deleted(record)
            deleted = True

        duration_ms = int((time.monotonic() - started) * 1000)
        log_archive_event(
            "worker_success",
            conversation_uuid=record.conversation_uuid,
            project_uuid=record.project_uuid,
            s3_key=s3_key,
            dry_run=dry_run,
            deleted=deleted,
            bytes_uploaded=len(gz_body),
            duration_ms=duration_ms,
        )
        return {
            "status": "success",
            "conversation_uuid": str(record.conversation_uuid),
            "s3_key": s3_key,
            "dry_run": dry_run,
            "deleted": deleted,
            "duration_ms": duration_ms,
        }
    except TransientS3Error:
        raise
    except Exception as exc:
        event_id = sentry_sdk.capture_exception(exc)
        errors = {"message": str(exc), "sentry_event_id": event_id}
        if record.status == ArchiveRecordStatus.IN_PROGRESS:
            ArchiveRecordStateMachine.transition_to_failed(record, errors=errors)
        log_archive_event(
            "worker_failed",
            conversation_uuid=record.conversation_uuid,
            project_uuid=record.project_uuid,
            sentry_event_id=event_id,
            error=str(exc),
        )
        return {"status": "failed", "sentry_event_id": event_id, "error": str(exc)}
