"""Archive record lifecycle transitions (make unreasonable states invalid)."""

from __future__ import annotations

from typing import Any

from django.db import transaction
from django.utils import timezone

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.models import ConversationArchiveRecord


class InvalidArchiveStateTransition(Exception):
    """Raised when a status transition is not allowed by the archive state graph."""


class InvalidArchiveRecordData(Exception):
    """Raised when transition payload fails validation before persisting."""


class ArchiveRecordStateMachine:
    """
    Allowed graph::

        PENDING → IN_PROGRESS → ARCHIVED → DELETED
           │            │           │
           └→ FAILED ←──┴───────────┘
                (retry → IN_PROGRESS)

        PENDING → FAILED is used when reclaiming abandoned/expired Celery tasks.
    """

    _ALLOWED: dict[str, frozenset[str]] = {
        # PENDING → FAILED: abandoned / expired Celery task reclaimed by dispatcher.
        ArchiveRecordStatus.PENDING: frozenset({ArchiveRecordStatus.IN_PROGRESS, ArchiveRecordStatus.FAILED}),
        ArchiveRecordStatus.IN_PROGRESS: frozenset({ArchiveRecordStatus.ARCHIVED, ArchiveRecordStatus.FAILED}),
        ArchiveRecordStatus.ARCHIVED: frozenset({ArchiveRecordStatus.DELETED}),
        ArchiveRecordStatus.FAILED: frozenset({ArchiveRecordStatus.IN_PROGRESS}),
        ArchiveRecordStatus.DELETED: frozenset(),
    }

    @classmethod
    def assert_transition(cls, from_status: str, to_status: str) -> None:
        allowed = cls._ALLOWED.get(from_status, frozenset())
        if to_status not in allowed:
            raise InvalidArchiveStateTransition(
                f"Cannot transition archive record from {from_status!r} to {to_status!r}"
            )

    @classmethod
    def _locked_record(cls, record: ConversationArchiveRecord) -> ConversationArchiveRecord:
        return ConversationArchiveRecord.objects.select_for_update().get(pk=record.pk)

    @staticmethod
    def _validate_archive_payload(*, s3_key: str, content_sha256: str) -> None:
        if not s3_key or not s3_key.strip():
            raise InvalidArchiveRecordData("s3_key is required for ARCHIVED transition")
        if not content_sha256 or not content_sha256.strip():
            raise InvalidArchiveRecordData("content_sha256 is required for ARCHIVED transition")

    @classmethod
    @transaction.atomic
    def transition_to_in_progress(cls, record: ConversationArchiveRecord) -> ConversationArchiveRecord:
        record = cls._locked_record(record)
        from_status = record.status
        cls.assert_transition(from_status, ArchiveRecordStatus.IN_PROGRESS)
        record.status = ArchiveRecordStatus.IN_PROGRESS
        update_fields = ["status"]
        if from_status == ArchiveRecordStatus.FAILED:
            record.failed_at = None
            record.finished_at = None
            record.errors = None
            update_fields.extend(["failed_at", "finished_at", "errors"])
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def transition_to_archived(
        cls,
        record: ConversationArchiveRecord,
        *,
        s3_key: str,
        content_sha256: str,
        dry_run: bool = False,
    ) -> ConversationArchiveRecord:
        record = cls._locked_record(record)
        cls.assert_transition(record.status, ArchiveRecordStatus.ARCHIVED)
        cls._validate_archive_payload(s3_key=s3_key, content_sha256=content_sha256)
        now = timezone.now()
        record.status = ArchiveRecordStatus.ARCHIVED
        record.s3_key = s3_key.strip()
        record.content_sha256 = content_sha256.strip()
        record.archived_at = now
        update_fields = ["status", "s3_key", "content_sha256", "archived_at"]
        if dry_run:
            record.finished_at = now
            update_fields.append("finished_at")
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def transition_to_deleted(cls, record: ConversationArchiveRecord) -> ConversationArchiveRecord:
        record = cls._locked_record(record)
        cls.assert_transition(record.status, ArchiveRecordStatus.DELETED)
        now = timezone.now()
        record.status = ArchiveRecordStatus.DELETED
        record.deleted_at = now
        record.finished_at = now
        record.save(update_fields=["status", "deleted_at", "finished_at"])
        return record

    @classmethod
    @transaction.atomic
    def transition_to_failed(
        cls,
        record: ConversationArchiveRecord,
        errors: dict[str, Any],
    ) -> ConversationArchiveRecord:
        record = cls._locked_record(record)
        cls.assert_transition(record.status, ArchiveRecordStatus.FAILED)
        now = timezone.now()
        record.status = ArchiveRecordStatus.FAILED
        record.failed_at = now
        record.finished_at = now
        record.errors = errors
        record.save(update_fields=["status", "failed_at", "finished_at", "errors"])
        return record
