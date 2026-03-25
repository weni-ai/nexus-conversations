"""
Tests for close_daily_conversations_task (dispatcher) and close_project_conversations_task (sub-task).
"""

from unittest.mock import Mock, patch

import pendulum
import pytest
from django.test import override_settings

from conversation_ms.close_daily.constants import (
    CLOSE_DAILY_LOCK_KEY,
    CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX,
    SYNC_PROJECT_TIMEZONES_LOCK_KEY,
)
from conversation_ms.close_daily.runner import (
    _determine_date_range,
    _is_conversation_already_processed,
    _process_conversation_batch,
    _process_project_conversations,
    _process_single_project,
)
from conversation_ms.models import ConversationMessages
from conversation_ms.tasks import close_daily_conversations_task, close_project_conversations_task
from conversation_ms.tests.factories import ConversationFactory, ProjectFactory, Resolution
from conversation_ms.utils.date_helpers import ProjectDay


@pytest.fixture
def mock_classification_service():
    """Mock ClassificationService."""
    service = Mock()
    service.classify_conversation = Mock()
    return service


@pytest.fixture
def mock_message_migration_service():
    """Mock MessageMigrationService."""
    service = Mock()
    service.migrate_conversation_messages_to_postgres = Mock()
    return service


@pytest.fixture
def mock_cache():
    """Mock Django cache."""
    cache = Mock()
    cache.add = Mock(return_value=True)
    cache.delete = Mock()
    return cache


@pytest.fixture
def mock_pendulum_now():
    """Mock pendulum.now() for time control."""
    with patch("conversation_ms.close_daily.runner.pendulum.now") as mock_now:
        yield mock_now


class TestCloseDailyConversationsTask:
    """Tests for close_daily_conversations_task."""

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_dispatcher_enqueues_project_subtasks(self, mock_cache):
        """Dispatcher scans projects and enqueues one sub-task per project."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run()

            assert result["status"] == "dispatched"
            assert result["projects_enqueued"] == 1
            mock_subtask.delay.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_dispatcher_enqueues_with_force_close(self, mock_cache):
        """Dispatcher forwards force_close to sub-tasks."""
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run(force_close=True)

            assert result["status"] == "dispatched"
            call_kwargs = mock_subtask.delay.call_args.kwargs
            assert call_kwargs["force_close"] is True

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_dispatcher_enqueues_with_date_range(self, mock_cache):
        """Dispatcher forwards start_date and end_date to sub-tasks."""
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run(
                force_close=True, start_date="2024-01-15", end_date="2024-01-17"
            )

            assert result["status"] == "dispatched"
            call_kwargs = mock_subtask.delay.call_args.kwargs
            assert call_kwargs["start_date"] == "2024-01-15"
            assert call_kwargs["end_date"] == "2024-01-17"

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_dispatcher_multiple_projects(self, mock_cache):
        """Dispatcher enqueues one sub-task per project."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="America/Sao_Paulo")
        ProjectFactory(timezone="America/New_York")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run()

            assert result["status"] == "dispatched"
            assert result["projects_enqueued"] == 2
            assert mock_subtask.delay.call_count == 2

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_day_ended_processes_conversations(self, mock_cache):
        """Test that conversations are processed when day ended."""
        project = ProjectFactory()

        mock_cache.add.return_value = True  # Day ended

        yesterday = pendulum.now("America/Sao_Paulo").subtract(days=1)
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=yesterday.start_of("day").in_timezone("UTC"),
        )

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.return_value = (
                conversation,
                None,
                Resolution.RESOLVED,
            )
            mock_class_service.return_value = mock_service

            conversations_closed = _process_project_conversations(
                str(project.uuid),
                "America/Sao_Paulo",
                ProjectDay.for_yesterday("America/Sao_Paulo").get_utc_range()[0],
                ProjectDay.for_yesterday("America/Sao_Paulo").get_utc_range()[1],
                force_close=False,
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db
    def test_automatic_single_project_always_attempts_processing(self):
        """No per-day cache skip: each scheduled run invokes the project processor."""
        project = ProjectFactory()
        project_data = {"uuid": str(project.uuid), "timezone": "America/Sao_Paulo"}

        with patch("conversation_ms.close_daily.runner._process_project_conversations", return_value=0) as mock_proc:
            conversations_closed, success = _process_single_project(
                project_data,
                "America/Sao_Paulo",
                force_close=False,
                start_date=None,
                end_date=None,
            )

        assert conversations_closed == 0
        assert success is True
        mock_proc.assert_called_once()

    @pytest.mark.django_db
    def test_conversation_already_processed_skips(self):
        """Test idempotency - already processed conversations are skipped."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.RESOLVED,
            end_date=project_day.get_end_date_utc(),
        )

        is_processed = _is_conversation_already_processed(str(conversation.uuid), project_day)

        assert is_processed is True

    @pytest.mark.django_db
    def test_conversation_not_processed_processes(self):
        """Test that unprocessed conversations are processed."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            end_date=None,
        )

        is_processed = _is_conversation_already_processed(str(conversation.uuid), project_day)

        assert is_processed is False

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_no_projects_in_database(self, mock_cache):
        """When there are no projects, the dispatcher enqueues nothing."""
        mock_cache.get.return_value = None
        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run()

            assert result["status"] == "dispatched"
            assert result["projects_enqueued"] == 0
            mock_subtask.delay.assert_not_called()

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_skips_when_timezone_sync_in_progress(self, mock_cache):
        """Scheduled close defers while sync_project_timezones_task holds the cache lock."""
        mock_cache.get.return_value = True
        result = close_daily_conversations_task.run()
        assert result["status"] == "skipped"
        assert result["reason"] == "sync_project_timezones_in_progress"

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_runs_when_sync_lock_held_if_skip_sync_lock_check(self, mock_cache):
        """Chained close from sync uses skip_sync_lock_check so it does not skip itself."""
        mock_cache.get.return_value = True
        ProjectFactory(timezone="America/Sao_Paulo")
        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks.close_project_conversations_task") as mock_subtask:
            result = close_daily_conversations_task.run(skip_sync_lock_check=True)

        assert result["status"] == "dispatched"
        mock_subtask.delay.assert_called_once()

    @pytest.mark.django_db
    def test_project_not_found_in_db(self):
        """Test handling when project doesn't exist in local database."""
        from uuid import uuid4

        project_uuid = str(uuid4())
        project_timezone = "America/Sao_Paulo"
        project_day = ProjectDay.for_yesterday(project_timezone)
        start_utc, end_utc = project_day.get_utc_range()

        conversations_closed = _process_project_conversations(
            project_uuid, project_timezone, start_utc, end_utc, force_close=False
        )

        assert conversations_closed == 0

    @pytest.mark.django_db
    def test_classification_error_continues(self):
        """Test that classification errors don't break batch processing."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversation1 = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )
        conversation2 = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = [
                (conversation1, None, Resolution.RESOLVED),
                Exception("Classification error"),
            ]
            mock_class_service.return_value = mock_service

            conversations_closed = _process_conversation_batch(
                [conversation1, conversation2],
                str(project.uuid),
                project_day.get_end_date_utc(),
            )

            assert conversations_closed >= 1

    @pytest.mark.django_db
    def test_batch_processing(self):
        """Test processing conversations in batches."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversations = ConversationFactory.create_batch(
            5,
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.return_value = (
                conversations[0],
                None,
                Resolution.RESOLVED,
            )
            mock_class_service.return_value = mock_service

            conversations_closed = _process_conversation_batch(
                conversations,
                str(project.uuid),
                project_day.get_end_date_utc(),
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db
    def test_batch_processing_sends_billing_sqs_when_queue_configured(self, settings):
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.test/q.fifo"
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )
        mock_producer = Mock()

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner.get_billing_sqs_producer", return_value=mock_producer):
                with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                    mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                        "persisted": False
                    }
                    _process_conversation_batch(
                        [conversation],
                        str(project.uuid),
                        project_day.get_end_date_utc(),
                    )

        mock_producer.send_conversation_close.assert_called_once()
        args, kwargs = mock_producer.send_conversation_close.call_args
        assert kwargs["message_deduplication_id"] == str(conversation.uuid)
        assert args[0]["resolution"] == Resolution.RESOLVED
        assert args[0]["contact_urn"] == conversation.contact_urn
        assert args[0]["channel_uuid"] == str(conversation.channel_uuid)
        assert args[0]["uuid"] == str(conversation.uuid)

    @pytest.mark.django_db
    def test_batch_processing_skips_billing_sqs_when_resolution_bulk_update_fails(self, settings):
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.test/q.fifo"
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )
        mock_producer = Mock()

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner.get_billing_sqs_producer", return_value=mock_producer):

                def _bulk_update_side_effect(objs, fields, batch_size=50):
                    if list(fields) == ["resolution"]:
                        raise RuntimeError("bulk_update failed")

                with patch(
                    "conversation_ms.close_daily.runner.Conversation.objects.bulk_update",
                    side_effect=_bulk_update_side_effect,
                ):
                    with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                        mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                            "persisted": False
                        }
                        _process_conversation_batch(
                            [conversation],
                            str(project.uuid),
                            project_day.get_end_date_utc(),
                        )

        mock_producer.send_conversation_close.assert_not_called()

    @pytest.mark.django_db
    def test_batch_processing_sends_datalake_events_after_resolution_persisted(self):
        """Datalake events are sent only after resolution bulk_update succeeds."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner._send_datalake_events") as mock_datalake:
                with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                    mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                        "persisted": False
                    }
                    _process_conversation_batch(
                        [conversation],
                        str(project.uuid),
                        project_day.get_end_date_utc(),
                    )

        mock_datalake.assert_called_once()
        sent_conversations = mock_datalake.call_args[0][0]
        assert len(sent_conversations) == 1
        assert sent_conversations[0].uuid == conversation.uuid

    @pytest.mark.django_db
    def test_batch_processing_skips_datalake_events_when_resolution_bulk_update_fails(self):
        """Datalake events are NOT sent when resolution bulk_update fails."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner._send_datalake_events") as mock_datalake:

                def _bulk_update_side_effect(objs, fields, batch_size=50):
                    if list(fields) == ["resolution"]:
                        raise RuntimeError("bulk_update failed")

                with patch(
                    "conversation_ms.close_daily.runner.Conversation.objects.bulk_update",
                    side_effect=_bulk_update_side_effect,
                ):
                    with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                        mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                            "persisted": False
                        }
                        _process_conversation_batch(
                            [conversation],
                            str(project.uuid),
                            project_day.get_end_date_utc(),
                        )

        mock_datalake.assert_not_called()

    @pytest.mark.django_db
    def test_project_processing_error_returns_failure(self):
        """Unhandled errors in project close are logged and reported as failure (no cache rollback)."""
        project = ProjectFactory()
        project_data = {"uuid": str(project.uuid), "timezone": "America/Sao_Paulo"}

        with patch("conversation_ms.close_daily.runner._process_project_conversations") as mock_process:
            mock_process.side_effect = Exception("Processing error")

            conversations_closed, success = _process_single_project(
                project_data,
                "America/Sao_Paulo",
                force_close=False,
                start_date=None,
                end_date=None,
            )

            assert conversations_closed == 0
            assert success is False

    @pytest.mark.django_db
    def test_conversation_with_has_chats_room(self):
        """Test conversation with has_chats_room flag."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            has_chats_room=True,
            start_date=project_day.start_of_day_utc,
        )

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.return_value = (
                conversation,
                None,
                Resolution.HAS_CHAT_ROOM,
            )
            mock_class_service.return_value = mock_service

            conversations_closed = _process_conversation_batch(
                [conversation],
                str(project.uuid),
                project_day.get_end_date_utc(),
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db
    def test_conversation_without_messages(self):
        """Test conversation without messages in DynamoDB."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.return_value = (conversation, None, None)
            mock_class_service.return_value = mock_service

            conversations_closed = _process_conversation_batch(
                [conversation],
                str(project.uuid),
                project_day.get_end_date_utc(),
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db(transaction=True)
    @override_settings(CLOSE_DAILY_CLASSIFICATION_THREADS=1)
    @patch("conversation_ms.close_daily.runner._queue_message_migrations")
    @patch("conversation_ms.services.message_migration_service.MessageRepository.get_messages_from_dynamo")
    def test_batch_persists_messages_before_classification_for_retry(
        self, mock_get_messages_from_dynamo, mock_queue_message_migrations
    ):
        """
        Integration coverage for retry-safety path:
        - messages are persisted to Postgres before classification
        - classification consumes preloaded messages
        - async migration queue is not required for that conversation
        """
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        persisted_messages = [
            {"text": "Oi", "source": "user", "created_at": "2026-03-23T12:00:00Z", "message_id": "msg-1"},
            {"text": "Tudo bem?", "source": "user", "created_at": "2026-03-23T12:00:05Z", "message_id": "msg-2"},
        ]
        mock_get_messages_from_dynamo.return_value = persisted_messages

        mock_classification_service = Mock()
        mock_classification_service.classify_conversation.return_value = (
            conversation,
            None,
            Resolution.RESOLVED,
        )

        conversations_closed = _process_conversation_batch(
            [conversation],
            str(project.uuid),
            project_day.get_end_date_utc(),
            classification_service=mock_classification_service,
            project_timezone="America/Sao_Paulo",
        )

        assert conversations_closed == 1

        messages_row = ConversationMessages.objects.get(conversation=conversation)
        assert len(messages_row.messages) == 2
        assert messages_row.messages[0]["text"] == "Oi"
        assert messages_row.messages[1]["text"] == "Tudo bem?"

        mock_classification_service.classify_conversation.assert_called_once()
        call_kwargs = mock_classification_service.classify_conversation.call_args.kwargs
        assert call_kwargs["messages_override"] is not None
        assert len(call_kwargs["messages_override"]) == 2

        mock_queue_message_migrations.assert_called_once_with([], str(project.uuid))

    @pytest.mark.django_db
    def test_determine_date_range_with_force_close(self):
        """Test _determine_date_range with force_close."""
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        target_date = project_day.get_date_string()

        start, end, project_day = _determine_date_range(
            force_close=True,
            start_date=None,
            end_date=None,
            target_date=target_date,
            project_timezone="America/Sao_Paulo",
        )

        assert isinstance(start, pendulum.DateTime)
        assert isinstance(end, pendulum.DateTime)
        assert project_day is not None

    @pytest.mark.django_db
    def test_determine_date_range_with_start_date(self):
        """Test _determine_date_range with start_date."""
        start, end, project_day = _determine_date_range(
            force_close=True,
            start_date="2024-01-15",
            end_date="2024-01-17",
            target_date="2024-01-15",
            project_timezone="America/Sao_Paulo",
        )

        assert isinstance(start, pendulum.DateTime)
        assert isinstance(end, pendulum.DateTime)
        assert project_day is None

    @pytest.mark.django_db
    @override_settings(CLOSE_DAILY_LOCK_ENABLED=True)
    @patch("conversation_ms.cache_access.cache")
    def test_close_daily_skips_when_distributed_lock_held(self, mock_cache):
        mock_cache.get.return_value = None

        def add_side_effect(key, *args, **kwargs):
            if key == CLOSE_DAILY_LOCK_KEY:
                return False
            if key == SYNC_PROJECT_TIMEZONES_LOCK_KEY:
                return True
            return True

        mock_cache.add = Mock(side_effect=add_side_effect)
        ProjectFactory(timezone="America/Sao_Paulo")

        result = close_daily_conversations_task.run(force_close=True, skip_sync_lock_check=True)

        assert result["status"] == "skipped"
        assert result["reason"] == "close_daily_already_running"

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_project_subtask_processes_project(self, mock_cache):
        """Sub-task processes a single project via run_close_project."""
        project = ProjectFactory(timezone="America/Sao_Paulo")
        mock_cache.add.return_value = True

        with patch("conversation_ms.close_daily.runner._process_single_project") as mock_single:
            mock_single.return_value = (5, True)

            result = close_project_conversations_task.run(
                project_uuid=str(project.uuid),
                project_timezone="America/Sao_Paulo",
            )

        assert result["status"] == "success"
        assert result["conversations_closed"] == 5
        mock_single.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_project_subtask_skips_when_project_lock_held(self, mock_cache):
        """Sub-task skips if another sub-task already holds the project lock."""
        project = ProjectFactory(timezone="America/Sao_Paulo")

        def add_side_effect(key, *args, **kwargs):
            if key.startswith(CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX):
                return False
            return True

        mock_cache.add = Mock(side_effect=add_side_effect)

        result = close_project_conversations_task.run(
            project_uuid=str(project.uuid),
            project_timezone="America/Sao_Paulo",
        )

        assert result["status"] == "skipped"
        assert result["reason"] == "project_already_running"

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_project_subtask_releases_lock_after_completion(self, mock_cache):
        """Sub-task releases the project lock after processing."""
        project = ProjectFactory(timezone="America/Sao_Paulo")
        mock_cache.add.return_value = True

        with patch("conversation_ms.close_daily.runner._process_single_project") as mock_single:
            mock_single.return_value = (0, True)

            close_project_conversations_task.run(
                project_uuid=str(project.uuid),
                project_timezone="America/Sao_Paulo",
            )

        expected_lock_key = f"{CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX}{project.uuid}"
        mock_cache.delete.assert_called_with(expected_lock_key)

    @pytest.mark.django_db
    @patch("conversation_ms.cache_access.cache")
    def test_project_subtask_reports_failure(self, mock_cache):
        """Sub-task reports failure status when _process_single_project fails."""
        project = ProjectFactory(timezone="America/Sao_Paulo")
        mock_cache.add.return_value = True

        with patch("conversation_ms.close_daily.runner._process_single_project") as mock_single:
            mock_single.return_value = (0, False)

            result = close_project_conversations_task.run(
                project_uuid=str(project.uuid),
                project_timezone="America/Sao_Paulo",
            )

        assert result["status"] == "failed"
        assert result["conversations_closed"] == 0


class TestThreadedClassification:
    """Tests for threaded classification within _process_conversation_batch."""

    @pytest.mark.django_db
    @override_settings(CLOSE_DAILY_CLASSIFICATION_THREADS=1)
    def test_single_thread_processes_all_conversations(self):
        """With threads=1 the batch still classifies every conversation."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversations = ConversationFactory.create_batch(
            3,
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                    "persisted": False
                }
                conversations_closed = _process_conversation_batch(
                    conversations,
                    str(project.uuid),
                    project_day.get_end_date_utc(),
                )

        assert conversations_closed == 3

    @pytest.mark.django_db
    @override_settings(CLOSE_DAILY_CLASSIFICATION_THREADS=3)
    def test_multiple_threads_process_all_conversations(self):
        """With threads>1, all conversations are classified regardless of execution order."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conversations = ConversationFactory.create_batch(
            5,
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        def _classify(conv, *args, **kwargs):
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                    "persisted": False
                }
                conversations_closed = _process_conversation_batch(
                    conversations,
                    str(project.uuid),
                    project_day.get_end_date_utc(),
                )

        assert conversations_closed == 5

    @pytest.mark.django_db
    @override_settings(CLOSE_DAILY_CLASSIFICATION_THREADS=2)
    def test_thread_exception_does_not_block_other_conversations(self):
        """An exception in one thread does not prevent other conversations from being processed."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        conv_ok = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )
        conv_fail = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=project_day.start_of_day_utc,
        )

        call_count = 0

        def _classify(conv, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if conv.uuid == conv_fail.uuid:
                raise RuntimeError("Lambda timeout")
            conv.resolution = Resolution.RESOLVED
            return (conv, None, Resolution.RESOLVED)

        with patch("conversation_ms.close_daily.runner.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.side_effect = _classify
            mock_class_service.return_value = mock_service
            with patch("conversation_ms.close_daily.runner.MessageMigrationService") as mock_migration_cls:
                mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
                    "persisted": False
                }
                conversations_closed = _process_conversation_batch(
                    [conv_ok, conv_fail],
                    str(project.uuid),
                    project_day.get_end_date_utc(),
                )

        assert call_count == 2
        assert conversations_closed == 1
