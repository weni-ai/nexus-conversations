"""Tests for archive dispatcher (lock, batch, expires)."""

import json
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
        CONVERSATION_ARCHIVE_LOCK_TTL_SECONDS=120,
        CONVERSATION_ARCHIVE_LOCK_HEARTBEAT_EVERY=2,
        CONVERSATION_ARCHIVE_LOCK_STALE_SECONDS=1800,
        CONVERSATION_ARCHIVE_STALE_RECORD_SECONDS=3600,
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


def _lock_json(*, started_at=None, batch_id=None) -> str:
    data = {"started_at": (started_at or timezone.now()).isoformat()}
    if batch_id is not None:
        data["batch_id"] = str(batch_id)
    return json.dumps(data, separators=(",", ":"))


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

    @patch("conversation_ms.archive.dispatcher.sentry_sdk")
    @patch("conversation_ms.cache_access.cache")
    def test_skips_when_lock_held_by_active_batch(self, mock_cache, mock_sentry, archive_settings):
        with archive_settings:
            owner = ConversationArchiveBatch.objects.create(
                started_at=timezone.now(),
                dry_run=True,
            )
            mock_cache.add.return_value = False
            mock_cache.get.return_value = _lock_json(started_at=owner.started_at, batch_id=owner.id)

            result = dispatch_archive_conversations(enqueue_task=Mock())

        assert result["status"] == "skipped"
        assert result["reason"] == "dispatcher_locked"
        mock_cache.delete.assert_not_called()
        mock_sentry.capture_message.assert_called_once()

    @patch("conversation_ms.cache_access.cache")
    def test_steals_lock_when_owner_batch_finished(self, mock_cache, archive_settings):
        with archive_settings:
            owner = ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(minutes=5),
                finished_at=timezone.now() - timedelta(minutes=4),
                enqueued_count=1,
                dry_run=True,
            )
            project = ProjectFactory(timezone="UTC")
            _eligible_conversation(project)

            mock_cache.add.side_effect = [False, True]
            mock_cache.get.return_value = _lock_json(started_at=owner.started_at, batch_id=owner.id)

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["status"] == "dispatched"
            assert result["enqueued_count"] == 1
            assert mock_cache.delete.call_count >= 1
            assert mock_cache.add.call_count == 2

    @patch("conversation_ms.cache_access.cache")
    def test_steals_lock_when_owner_batch_stale(self, mock_cache, archive_settings):
        with archive_settings:
            owner = ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(hours=2),
                dry_run=True,
            )
            project = ProjectFactory(timezone="UTC")
            _eligible_conversation(project)

            mock_cache.add.side_effect = [False, True]
            mock_cache.get.return_value = _lock_json(started_at=owner.started_at, batch_id=owner.id)

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["status"] == "dispatched"
            assert result["enqueued_count"] == 1
            owner.refresh_from_db()
            assert owner.finished_at is not None

    @patch("conversation_ms.cache_access.cache")
    def test_steals_opaque_lock_when_no_open_batch(self, mock_cache, archive_settings):
        with archive_settings:
            project = ProjectFactory(timezone="UTC")
            _eligible_conversation(project)

            mock_cache.add.side_effect = [False, True]
            mock_cache.get.return_value = "1"

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["status"] == "dispatched"
            assert result["enqueued_count"] == 1

    @patch("conversation_ms.cache_access.cache")
    def test_steals_opaque_lock_when_open_batch_stale(self, mock_cache, archive_settings):
        with archive_settings:
            zombie = ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(hours=2),
                dry_run=True,
            )
            project = ProjectFactory(timezone="UTC")
            _eligible_conversation(project)

            mock_cache.add.side_effect = [False, True]
            mock_cache.get.return_value = "1"

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["status"] == "dispatched"
            zombie.refresh_from_db()
            assert zombie.finished_at is not None

    @patch("conversation_ms.archive.dispatcher.sentry_sdk")
    @patch("conversation_ms.cache_access.cache")
    def test_keeps_opaque_lock_when_open_batch_recent(self, mock_cache, mock_sentry, archive_settings):
        with archive_settings:
            ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(minutes=5),
                dry_run=True,
            )
            mock_cache.add.return_value = False
            mock_cache.get.return_value = "1"

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

            mock_cache.set.assert_called()
            mock_cache.delete.assert_called_with(ARCHIVE_DISPATCHER_LOCK_KEY)

    @patch("conversation_ms.cache_access.cache")
    def test_heartbeats_lock_during_enqueue(self, mock_cache, archive_settings):
        with archive_settings, override_settings(CONVERSATION_ARCHIVE_BATCH_SIZE=5):
            mock_cache.add.return_value = True
            project = ProjectFactory(timezone="UTC")
            for _ in range(5):
                _eligible_conversation(project)

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["enqueued_count"] == 5
            assert mock_cache.set.call_count >= 3

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

    @patch("conversation_ms.cache_access.cache")
    def test_reclaims_stale_pending_and_reenqueues(self, mock_cache, archive_settings):
        with archive_settings, override_settings(CONVERSATION_ARCHIVE_STALE_RECORD_SECONDS=60):
            mock_cache.add.return_value = True
            project = ProjectFactory(timezone="UTC")
            conversation = _eligible_conversation(project)
            old_batch = ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(hours=2),
                finished_at=timezone.now() - timedelta(hours=2),
                enqueued_count=1,
                dry_run=True,
            )
            stale = ConversationArchiveRecord.objects.create(
                conversation_uuid=conversation.uuid,
                project_uuid=project.uuid,
                batch=old_batch,
                status=ArchiveRecordStatus.PENDING,
                started_at=timezone.now() - timedelta(hours=2),
            )

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["enqueued_count"] == 1
            stale.refresh_from_db()
            assert stale.status == ArchiveRecordStatus.FAILED
            assert stale.errors["reason"] == "stale_in_flight"
            enqueue_task.apply_async.assert_called_once_with(
                args=[str(stale.id)],
                queue="conversations-archive",
                expires=enqueue_task.apply_async.call_args.kwargs["expires"],
            )

    @patch("conversation_ms.cache_access.cache")
    def test_closes_zombie_unfinished_batches(self, mock_cache, archive_settings):
        with archive_settings:
            mock_cache.add.return_value = True
            zombie = ConversationArchiveBatch.objects.create(
                started_at=timezone.now() - timedelta(hours=2),
                dry_run=True,
            )
            ConversationArchiveRecord.objects.create(
                conversation_uuid=uuid4(),
                project_uuid=uuid4(),
                batch=zombie,
                status=ArchiveRecordStatus.ARCHIVED,
                started_at=timezone.now() - timedelta(hours=2),
                archived_at=timezone.now() - timedelta(hours=2),
                finished_at=timezone.now() - timedelta(hours=2),
                s3_key="conversations-archive/p/2026/01/u.json.gz",
                content_sha256="a" * 64,
            )
            project = ProjectFactory(timezone="UTC")
            _eligible_conversation(project)

            enqueue_task = Mock()
            enqueue_task.apply_async = Mock(return_value=Mock(id="task-1"))
            result = dispatch_archive_conversations(enqueue_task=enqueue_task)

            assert result["status"] == "dispatched"
            zombie.refresh_from_db()
            assert zombie.finished_at is not None
            assert zombie.enqueued_count == 1
