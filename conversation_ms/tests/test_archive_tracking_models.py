"""Tests for archive tracking models and DB constraints."""

from uuid import uuid4

import pytest
from django.db import IntegrityError
from django.utils import timezone

from conversation_ms.archive.constants import ArchiveRecordStatus
from conversation_ms.models import ConversationArchiveBatch, ConversationArchiveRecord


@pytest.fixture
def archive_batch():
    return ConversationArchiveBatch.objects.create(
        started_at=timezone.now(),
        dry_run=True,
    )


def _pending_record(batch, conversation_uuid=None, project_uuid=None):
    return ConversationArchiveRecord.objects.create(
        conversation_uuid=conversation_uuid or uuid4(),
        project_uuid=project_uuid or uuid4(),
        batch=batch,
        status=ArchiveRecordStatus.PENDING,
        started_at=timezone.now(),
    )


@pytest.mark.django_db
class TestConversationArchiveBatch:
    def test_create_batch(self):
        batch = ConversationArchiveBatch.objects.create(
            started_at=timezone.now(),
            enqueued_count=10,
            dry_run=False,
        )
        assert batch.enqueued_count == 10
        assert batch.finished_at is None


@pytest.mark.django_db
class TestConversationArchiveRecordConstraints:
    def test_unique_conversation_uuid(self, archive_batch):
        conv_uuid = uuid4()
        _pending_record(archive_batch, conversation_uuid=conv_uuid)
        with pytest.raises(IntegrityError):
            _pending_record(archive_batch, conversation_uuid=conv_uuid)

    def test_archived_requires_s3_key_and_archived_at(self, archive_batch):
        with pytest.raises(IntegrityError):
            ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=archive_batch,
                status=ArchiveRecordStatus.ARCHIVED,
                started_at=timezone.now(),
            )

    def test_deleted_requires_deleted_at(self, archive_batch):
        now = timezone.now()
        with pytest.raises(IntegrityError):
            ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=archive_batch,
                status=ArchiveRecordStatus.DELETED,
                started_at=now,
                s3_key="conversations-archive/p/u/m/c.json.gz",
                archived_at=now,
                finished_at=now,
            )

    def test_failed_requires_failed_at(self, archive_batch):
        with pytest.raises(IntegrityError):
            ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=archive_batch,
                status=ArchiveRecordStatus.FAILED,
                started_at=timezone.now(),
                finished_at=timezone.now(),
                errors={"message": "boom"},
            )

    def test_pending_cannot_have_finished_at(self, archive_batch):
        with pytest.raises(IntegrityError):
            ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=archive_batch,
                status=ArchiveRecordStatus.PENDING,
                started_at=timezone.now(),
                finished_at=timezone.now(),
            )
