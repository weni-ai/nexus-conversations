"""Tests for ArchiveRecordStateMachine transitions."""

from uuid import uuid4

import pytest
from django.utils import timezone

from conversation_ms.archive.constants import (
    DISPATCHER_SKIP_STATUSES,
    DRY_RUN_TERMINAL_STATUS,
    FINISHED_AT_REQUIRED_STATUSES,
    IN_FLIGHT_ARCHIVE_STATUSES,
    ArchiveRecordStatus,
)
from conversation_ms.archive.state_machine import (
    ArchiveRecordStateMachine,
    InvalidArchiveRecordData,
    InvalidArchiveStateTransition,
)
from conversation_ms.models import ConversationArchiveBatch, ConversationArchiveRecord


@pytest.fixture
def archive_batch():
    return ConversationArchiveBatch.objects.create(
        started_at=timezone.now(),
        dry_run=True,
    )


def _pending_record(batch):
    return ConversationArchiveRecord.objects.create(
        conversation_uuid=uuid4(),
        project_uuid=uuid4(),
        batch=batch,
        status=ArchiveRecordStatus.PENDING,
        started_at=timezone.now(),
    )


@pytest.mark.django_db
class TestArchiveRecordStateMachine:
    def test_happy_path_dry_run(self, archive_batch):
        record = _pending_record(archive_batch)
        ArchiveRecordStateMachine.transition_to_in_progress(record)
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.IN_PROGRESS

        ArchiveRecordStateMachine.transition_to_archived(
            record,
            s3_key="conversations-archive/p/2026/04/u.json.gz",
            content_sha256="a" * 64,
            dry_run=True,
        )
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.ARCHIVED
        assert record.finished_at is not None
        assert record.deleted_at is None

    def test_happy_path_with_delete(self, archive_batch):
        record = _pending_record(archive_batch)
        ArchiveRecordStateMachine.transition_to_in_progress(record)
        ArchiveRecordStateMachine.transition_to_archived(
            record,
            s3_key="conversations-archive/p/2026/04/u.json.gz",
            content_sha256="b" * 64,
            dry_run=False,
        )
        record.refresh_from_db()
        assert record.finished_at is None

        ArchiveRecordStateMachine.transition_to_deleted(record)
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.DELETED
        assert record.deleted_at is not None
        assert record.finished_at is not None

    def test_failure_and_retry(self, archive_batch):
        record = _pending_record(archive_batch)
        ArchiveRecordStateMachine.transition_to_in_progress(record)
        ArchiveRecordStateMachine.transition_to_failed(
            record,
            errors={"message": "S3 timeout", "sentry_event_id": "evt-1"},
        )
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.FAILED

        ArchiveRecordStateMachine.transition_to_in_progress(record)
        record.refresh_from_db()
        assert record.status == ArchiveRecordStatus.IN_PROGRESS
        assert record.failed_at is None
        assert record.errors is None

    @pytest.mark.parametrize(
        ("from_status", "to_method"),
        [
            (ArchiveRecordStatus.PENDING, "transition_to_deleted"),
            (ArchiveRecordStatus.IN_PROGRESS, "transition_to_deleted"),
            (ArchiveRecordStatus.FAILED, "transition_to_deleted"),
            (ArchiveRecordStatus.ARCHIVED, "transition_to_in_progress"),
        ],
    )
    def test_invalid_transitions_raise(self, archive_batch, from_status, to_method):
        record = _pending_record(archive_batch)
        record.status = from_status
        if from_status == ArchiveRecordStatus.ARCHIVED:
            now = timezone.now()
            record.s3_key = "key"
            record.archived_at = now
        if from_status == ArchiveRecordStatus.FAILED:
            record.failed_at = timezone.now()
            record.finished_at = timezone.now()
            record.errors = {"message": "x"}
        record.save()

        with pytest.raises(InvalidArchiveStateTransition):
            if to_method == "transition_to_deleted":
                ArchiveRecordStateMachine.transition_to_deleted(record)
            elif to_method == "transition_to_in_progress":
                ArchiveRecordStateMachine.transition_to_in_progress(record)

    def test_assert_transition_invalid(self):
        with pytest.raises(InvalidArchiveStateTransition):
            ArchiveRecordStateMachine.assert_transition(
                ArchiveRecordStatus.PENDING,
                ArchiveRecordStatus.DELETED,
            )

    def test_transition_to_archived_rejects_empty_s3_key(self, archive_batch):
        record = _pending_record(archive_batch)
        ArchiveRecordStateMachine.transition_to_in_progress(record)
        with pytest.raises(InvalidArchiveRecordData):
            ArchiveRecordStateMachine.transition_to_archived(
                record,
                s3_key="   ",
                content_sha256="a" * 64,
            )

    def test_transition_to_archived_rejects_empty_content_sha256(self, archive_batch):
        record = _pending_record(archive_batch)
        ArchiveRecordStateMachine.transition_to_in_progress(record)
        with pytest.raises(InvalidArchiveRecordData):
            ArchiveRecordStateMachine.transition_to_archived(
                record,
                s3_key="conversations-archive/p/2026/04/u.json.gz",
                content_sha256="",
            )


@pytest.mark.django_db
class TestArchiveStatusConstants:
    def test_in_flight_excludes_archived_and_deleted(self):
        assert ArchiveRecordStatus.ARCHIVED not in IN_FLIGHT_ARCHIVE_STATUSES
        assert ArchiveRecordStatus.DELETED not in IN_FLIGHT_ARCHIVE_STATUSES
        assert IN_FLIGHT_ARCHIVE_STATUSES == frozenset({ArchiveRecordStatus.PENDING, ArchiveRecordStatus.IN_PROGRESS})

    def test_dispatcher_skip_matches_spec(self):
        assert DISPATCHER_SKIP_STATUSES == frozenset(
            {
                ArchiveRecordStatus.PENDING,
                ArchiveRecordStatus.IN_PROGRESS,
                ArchiveRecordStatus.ARCHIVED,
                ArchiveRecordStatus.DELETED,
            }
        )
        assert ArchiveRecordStatus.FAILED not in DISPATCHER_SKIP_STATUSES

    def test_finished_at_required_excludes_archived(self):
        assert ArchiveRecordStatus.ARCHIVED not in FINISHED_AT_REQUIRED_STATUSES
        assert DRY_RUN_TERMINAL_STATUS == ArchiveRecordStatus.ARCHIVED
