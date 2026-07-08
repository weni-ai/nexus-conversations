"""Shared constants for conversation archive and retention."""

from django.db import models

RESOLUTION_IN_PROGRESS = "2"


class ArchiveRecordStatus(models.TextChoices):
    PENDING = "PENDING", "Pending"
    IN_PROGRESS = "IN_PROGRESS", "In progress"
    ARCHIVED = "ARCHIVED", "Archived"
    DELETED = "DELETED", "Deleted"
    FAILED = "FAILED", "Failed"


# Worker still processing (not yet terminal for any mode).
IN_FLIGHT_ARCHIVE_STATUSES = frozenset(
    {
        ArchiveRecordStatus.PENDING,
        ArchiveRecordStatus.IN_PROGRESS,
    }
)

# Dispatcher must not create a new record / enqueue when status is in this set.
DISPATCHER_SKIP_STATUSES = frozenset(
    {
        ArchiveRecordStatus.PENDING,
        ArchiveRecordStatus.IN_PROGRESS,
        ArchiveRecordStatus.ARCHIVED,
        ArchiveRecordStatus.DELETED,
    }
)

# Dry-run stops at ARCHIVED with finished_at set (no Postgres delete).
DRY_RUN_TERMINAL_STATUS = ArchiveRecordStatus.ARCHIVED

# finished_at is required in DB for these statuses (see model CheckConstraints).
FINISHED_AT_REQUIRED_STATUSES = frozenset(
    {
        ArchiveRecordStatus.DELETED,
        ArchiveRecordStatus.FAILED,
    }
)

RETRY_ELIGIBLE_ARCHIVE_STATUSES = frozenset({ArchiveRecordStatus.FAILED})

ARCHIVE_DISPATCHER_LOCK_KEY = "conversation_ms:archive_dispatcher_active"
