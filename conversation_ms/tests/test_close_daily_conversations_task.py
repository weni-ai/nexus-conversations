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
        ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            start_date=yesterday.start_of("day").in_timezone("UTC"),
        )

        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            conversations_closed = _process_project_conversations(
                str(project.uuid),
                "America/Sao_Paulo",
                ProjectDay.for_yesterday("America/Sao_Paulo").get_utc_range()[0],
                ProjectDay.for_yesterday("America/Sao_Paulo").get_utc_range()[1],
                force_close=False,
            )

            assert conversations_closed >= 1
            assert mock_enqueue.call_count >= 1

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
        """One claim failure does not block other conversations in the batch."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation1 = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        conversation2 = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )

        from conversation_ms.close_daily import state_machine as sm_mod

        real_claim = sm_mod.ClosePipelineStateMachine.claim_classify.__func__

        def _claim(conversation):
            if conversation.uuid == conversation1.uuid:
                raise RuntimeError("claim failed")
            return real_claim(sm_mod.ClosePipelineStateMachine, conversation)

        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            with patch(
                "conversation_ms.close_daily.runner.ClosePipelineStateMachine.claim_classify",
                side_effect=_claim,
            ):
                conversations_closed = _process_conversation_batch(
                    [conversation1, conversation2], str(project.uuid), project_day.get_end_date_utc()
                )
        assert conversations_closed == 1
        mock_enqueue.assert_called_once_with(str(conversation2.uuid))

    @pytest.mark.django_db
    def test_batch_processing(self):
        """Selector claims and enqueues classify for each conversation."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversations = ConversationFactory.create_batch(
            5, project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            conversations_closed = _process_conversation_batch(
                conversations, str(project.uuid), project_day.get_end_date_utc()
            )
        assert conversations_closed == 5
        assert mock_enqueue.call_count == 5

    @pytest.mark.django_db
    def test_batch_processing_sends_billing_sqs_when_queue_configured(self, settings):
        """Selector no longer sends billing; it only enqueues classify."""
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.test/q.fifo"
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            closed = _process_conversation_batch([conversation], str(project.uuid), project_day.get_end_date_utc())
        assert closed == 1
        mock_enqueue.assert_called_once_with(str(conversation.uuid))

    @pytest.mark.django_db
    def test_batch_processing_skips_billing_sqs_when_resolution_bulk_update_fails(self, settings):
        """Billing is not invoked from the selector after cutover."""
        settings.SQS_BILLING_QUEUE_URL = "https://sqs.test/q.fifo"
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            closed = _process_conversation_batch([conversation], str(project.uuid), project_day.get_end_date_utc())
        assert closed == 1
        mock_enqueue.assert_called_once()

    @pytest.mark.django_db
    def test_batch_processing_sends_datalake_events_after_resolution_persisted(self):
        """Datalake is not invoked from the selector after cutover."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            closed = _process_conversation_batch([conversation], str(project.uuid), project_day.get_end_date_utc())
        assert closed == 1
        mock_enqueue.assert_called_once()

    @pytest.mark.django_db
    def test_batch_processing_skips_datalake_events_when_resolution_bulk_update_fails(self):
        """Datalake is not invoked from the selector after cutover."""
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            closed = _process_conversation_batch([conversation], str(project.uuid), project_day.get_end_date_utc())
        assert closed == 1
        mock_enqueue.assert_called_once()

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
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project,
            resolution=Resolution.IN_PROGRESS,
            has_chats_room=True,
            start_date=project_day.start_of_day_utc,
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            conversations_closed = _process_conversation_batch(
                [conversation], str(project.uuid), project_day.get_end_date_utc()
            )
        assert conversations_closed == 1
        mock_enqueue.assert_called_once_with(str(conversation.uuid))

    @pytest.mark.django_db
    def test_conversation_without_messages(self):
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversation = ConversationFactory(
            project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            conversations_closed = _process_conversation_batch(
                [conversation], str(project.uuid), project_day.get_end_date_utc()
            )
        assert conversations_closed == 1
        mock_enqueue.assert_called_once()

    @pytest.mark.django_db(transaction=True)
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


class TestSelectorClassificationCutover:
    @pytest.mark.django_db
    def test_batch_claims_all_conversations(self):
        project = ProjectFactory()
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        conversations = ConversationFactory.create_batch(
            3, project=project, resolution=Resolution.IN_PROGRESS, start_date=project_day.start_of_day_utc
        )
        with patch("conversation_ms.close_daily.runner.enqueue_classify") as mock_enqueue:
            conversations_closed = _process_conversation_batch(
                conversations, str(project.uuid), project_day.get_end_date_utc()
            )
        assert conversations_closed == 3
        assert mock_enqueue.call_count == 3
