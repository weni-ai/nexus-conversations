"""Tests for archive dispatcher (lock, batch, expires)."""

from datetime import timedelta
from unittest.mock import Mock, patch
from uuid import uuid4

import pendulum
import pytest
from django.test import override_settings
from django.utils import timezone

from conversation_ms.archive.constants import ARCHIVE_DISPATCHER_LOCK_KEY, ArchiveRecordStatus
from conversation_ms.archive.dispatcher import dispatch_archive_conversations
from conversation_ms.models import ConversationArchiveBatch, ConversationArchiveRecord, ConversationMessages
from conversation_ms.tests.factories import ConversationFactory, ProjectFactory, Resolution


@pytest.fixture
def archive_settings():
    return override_settings(
        CONVERSATION_ARCHIVE_ENABLED=True,
        CONVERSATION_ARCHIVE_DRY_RUN=True,
        CONVERSATION_ARCHIVE_S3_BUCKET="test-archive-bucket",
        CONVERSATION_ARCHIVE_BATCH_SIZE=10,
        CONVERSATION_ARCHIVE_TASK_EXPIRES_SECONDS=1800,
        CONVERSATION_ARCHIVE_CELERY_QUEUE="conversations-archive",
        CONVERSATION_ARCHIVE_LOCK_ENABLED=True,
        CONVERSATION_RETENTION_DAYS=90,
    )


def _eligible_conversation(project, *, days_ago=120):
    now = pendulum.now("UTC")
    conversation = ConversationFactory(
        project=project,
        resolution=Resolution.RESOLVED,
        start_date=now.subtract(days=days_ago).naive(),
        end_date=now.subtract(days=days_ago).naive(),
        created_at=now.subtract(days=days_ago).naive(),
    )
    ConversationMessages.objects.create(
        conversation=conversation,
        messages=[{"text": "hello", "source": "user", "created_at": "2026-01-01T00:00:00Z"}],
    )
    return conversation


@pytest.mark.django_db
class TestArchiveDispatcher:
    @patch("conversation_ms.cache_access.cache")
    def test_skips_when_archive_disabled(self, mock_cache):
        mock_cache.add.return_value = True
        result = dispatch_archive_conversations(enqueue_task=Mock())
        assert result["status"] == "skipped"
        assert result["reason"] == "archive_disabled"

    @patch("conversation_ms.cache_access.cache")
    def test_skips_when_bucket_missing(self, mock_cache, archive_settings):
        with archive_settings, override_settings(CONVERSATION_ARCHIVE_S3_BUCKET=""):
            mock_cache.add.return_value = True
            result = dispatch_archive_conversations(enqueue_task=Mock())
        assert result["status"] == "skipped"
        assert result["reason"] == "missing_s3_bucket"

    @patch("conversation_ms.cache_access.cache")
    def test_skips_when_lock_held(self, mock_cache, archive_settings):
        with archive_settings:
            mock_cache.add.return_value = False
            result = dispatch_archive_conversations(enqueue_task=Mock())
        assert result["status"] == "skipped"
        assert result["reason"] == "dispatcher_locked"

    @patch("conversation_ms.cache_access.cache")
    def test_enqueues_with_expires_and_creates_pending_records(self, mock_cache, archive_settings):
        with archive_settings:
            mock_cache.add.return_value = True
            project = ProjectFactory(timezone="America/Sao_Paulo")
            conversation = _eligible_conversation(project)

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))

            before = timezone.now()
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)
            after = timezone.now()

            assert result["status"] == "dispatched"
            assert result["enqueued_count"] == 1
            assert ConversationArchiveBatch.objects.count() == 1

            record = ConversationArchiveRecord.objects.get(conversation_uuid=conversation.uuid)
            assert record.status == ArchiveRecordStatus.PENDING

            enqueue_task.apply_async.assert_called_once()
            call_kwargs = enqueue_task.apply_async.call_args.kwargs
            assert call_kwargs["queue"] == "conversations-archive"
            assert call_kwargs["args"] == [str(record.id)]
            expires = call_kwargs["expires"]
            assert before + timedelta(seconds=1800) <= expires <= after + timedelta(seconds=1800)

            mock_cache.delete.assert_called_with(ARCHIVE_DISPATCHER_LOCK_KEY)

    @patch("conversation_ms.cache_access.cache")
    def test_skips_conversations_with_active_records(self, mock_cache, archive_settings):
        with archive_settings:
            mock_cache.add.return_value = True
            project = ProjectFactory(timezone="UTC")
            conversation = _eligible_conversation(project)
            batch = ConversationArchiveBatch.objects.create(started_at=timezone.now(), dry_run=True)
            ConversationArchiveRecord.objects.create(
                conversation_uuid=conversation.uuid,
                project_uuid=project.uuid,
                batch=batch,
                status=ArchiveRecordStatus.PENDING,
                started_at=timezone.now(),
            )

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock()

            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["enqueued_count"] == 0
            enqueue_task.apply_async.assert_not_called()

    @patch("conversation_ms.cache_access.cache")
    def test_retries_failed_records(self, mock_cache, archive_settings):
        with archive_settings:
            mock_cache.add.return_value = True
            batch = ConversationArchiveBatch.objects.create(started_at=timezone.now(), dry_run=True)
            failed_record = ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=batch,
                status=ArchiveRecordStatus.FAILED,
                started_at=timezone.now(),
                failed_at=timezone.now(),
                finished_at=timezone.now(),
                errors={"message": "boom"},
            )

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))

            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["enqueued_count"] == 1
            enqueue_task.apply_async.assert_called_once_with(
                args=[str(failed_record.id)],
                queue="conversations-archive",
                expires=enqueue_task.apply_async.call_args.kwargs["expires"],
            )
