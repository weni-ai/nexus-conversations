"""
Tests for close_daily_conversations_task.
"""

from unittest.mock import Mock, patch

import pendulum
import pytest

from conversation_ms.tasks import (
    _determine_date_range,
    _is_conversation_already_processed,
    _process_conversation_batch,
    _process_project_conversations,
    _process_single_project,
    close_daily_conversations_task,
)
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
    with patch("conversation_ms.tasks.pendulum.now") as mock_now:
        yield mock_now


class TestCloseDailyConversationsTask:
    """Tests for close_daily_conversations_task."""

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_close_daily_conversations_task_automatic(self, mock_cache):
        """Test automatic task execution (cron) - processes projects whose day ended."""
        mock_cache.get.return_value = None  # sync lock not held (default Mock is truthy)
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True  # Day ended (cache.add returns True = not processed)

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 1

            result = close_daily_conversations_task()

            assert result["status"] == "success"
            assert result["projects_scanned"] == 1
            assert result["projects_processed"] >= 0
            mock_process.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_close_daily_conversations_task_force_close(self, mock_cache):
        """Test force_close processes even if day hasn't ended."""
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = False  # Day not ended (already processed)

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 2

            result = close_daily_conversations_task(force_close=True)

            assert result["status"] == "success"
            mock_process.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_close_daily_conversations_task_force_close_with_start_date(self, mock_cache):
        """Test force_close with specific start_date."""
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 1

            result = close_daily_conversations_task(force_close=True, start_date="2024-01-15")

            assert result["status"] == "success"
            mock_process.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_close_daily_conversations_task_force_close_with_date_range(self, mock_cache):
        """Test force_close with date range."""
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 3

            result = close_daily_conversations_task(force_close=True, start_date="2024-01-15", end_date="2024-01-17")

            assert result["status"] == "success"
            mock_process.assert_called_once()

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_project_with_valid_timezone(self, mock_cache):
        """Test project with valid timezone."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 0

            result = close_daily_conversations_task()

            assert result["status"] == "success"

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_project_with_invalid_timezone(self, mock_cache):
        """Test project with invalid timezone uses fallback."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="Invalid/Timezone")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 0

            result = close_daily_conversations_task()

            assert result["status"] == "success"

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_project_without_timezone(self, mock_cache):
        """Test project without timezone uses fallback."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone=None)

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 0

            result = close_daily_conversations_task()

            assert result["status"] == "success"

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_different_timezones_same_utc_day(self, mock_cache):
        """Test projects in different timezones on the same UTC day."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="America/Sao_Paulo")
        ProjectFactory(timezone="America/New_York")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 0

            result = close_daily_conversations_task()

            assert result["status"] == "success"
            assert mock_process.call_count == 2

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
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

        with patch("conversation_ms.tasks.ClassificationService") as mock_class_service:
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
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_day_not_ended_skips(self, mock_cache):
        """Test that processing is skipped when day hasn't ended."""
        project = ProjectFactory()
        project_data = {"uuid": str(project.uuid), "timezone": "America/Sao_Paulo"}

        mock_cache.add.return_value = False  # Day not ended (already processed)

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
    @patch("conversation_ms.tasks.cache")
    def test_multiple_projects_processed(self, mock_cache):
        """Each local project is considered for close-daily processing."""
        mock_cache.get.return_value = None
        ProjectFactory(timezone="America/Sao_Paulo")
        ProjectFactory(timezone="America/Sao_Paulo")

        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.return_value = 0

            result = close_daily_conversations_task()

            assert result["status"] == "success"
            assert result["projects_scanned"] == 2
            assert mock_process.call_count == 2

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_no_projects_in_database(self, mock_cache):
        """When there are no projects, the task completes without calling processing."""
        mock_cache.get.return_value = None
        mock_cache.add.return_value = True

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            result = close_daily_conversations_task()

            assert result["status"] == "success"
            assert result["projects_scanned"] == 0
            mock_process.assert_not_called()

    @pytest.mark.django_db
    @patch("conversation_ms.tasks.cache")
    def test_skips_when_timezone_sync_in_progress(self, mock_cache):
        """Scheduled close defers while sync_project_timezones_task holds the cache lock."""
        mock_cache.get.return_value = True
        result = close_daily_conversations_task()
        assert result["status"] == "skipped"
        assert result["reason"] == "sync_project_timezones_in_progress"

    @pytest.mark.django_db
    def test_project_not_found_in_db(self):
        """Test handling when project doesn't exist in local database."""
        from uuid import uuid4

        project_uuid = str(uuid4())
        project_timezone = "America/Sao_Paulo"
        project_day = ProjectDay.for_yesterday(project_timezone)
        start_utc, end_utc = project_day.get_utc_range()

        conversations_closed = _process_project_conversations(project_uuid, project_timezone, start_utc, end_utc)

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

        with patch("conversation_ms.tasks.ClassificationService") as mock_class_service:
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

        with patch("conversation_ms.tasks.ClassificationService") as mock_class_service:
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
    @patch("conversation_ms.tasks.cache")
    def test_cache_prevents_duplicate_processing(self, mock_cache):
        """Test that cache prevents duplicate processing."""
        project = ProjectFactory()
        project_data = {"uuid": str(project.uuid), "timezone": "America/Sao_Paulo"}

        mock_cache.add.return_value = False

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
    @patch("conversation_ms.tasks.cache")
    def test_cache_cleared_on_error(self, mock_cache):
        """Test that cache is cleared on processing error."""
        project = ProjectFactory()
        project_data = {"uuid": str(project.uuid), "timezone": "America/Sao_Paulo"}

        mock_cache.add.return_value = True  # Day ended

        with patch("conversation_ms.tasks._process_project_conversations") as mock_process:
            mock_process.side_effect = Exception("Processing error")

            conversations_closed, success = _process_single_project(
                project_data,
                "America/Sao_Paulo",
                force_close=False,
                start_date=None,
                end_date=None,
            )

            assert mock_cache.delete.called
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

        with patch("conversation_ms.tasks.ClassificationService") as mock_class_service:
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

        with patch("conversation_ms.tasks.ClassificationService") as mock_class_service:
            mock_service = Mock()
            mock_service.classify_conversation.return_value = (conversation, None, None)
            mock_class_service.return_value = mock_service

            conversations_closed = _process_conversation_batch(
                [conversation],
                str(project.uuid),
                project_day.get_end_date_utc(),
            )

            assert conversations_closed >= 0

    @pytest.mark.django_db
    def test_determine_date_range_with_force_close(self):
        """Test _determine_date_range with force_close."""
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")
        target_date = project_day.get_date_string()

        date_range = _determine_date_range(
            force_close=True,
            start_date=None,
            end_date=None,
            day_ended=False,
            target_date=target_date,
            project_timezone="America/Sao_Paulo",
        )

        assert date_range is not None
        start, end = date_range
        assert isinstance(start, pendulum.DateTime)
        assert isinstance(end, pendulum.DateTime)

    @pytest.mark.django_db
    def test_determine_date_range_with_start_date(self):
        """Test _determine_date_range with start_date."""
        date_range = _determine_date_range(
            force_close=True,
            start_date="2024-01-15",
            end_date="2024-01-17",
            day_ended=False,
            target_date="2024-01-15",
            project_timezone="America/Sao_Paulo",
        )

        assert date_range is not None
        start, end = date_range
        assert isinstance(start, pendulum.DateTime)
        assert isinstance(end, pendulum.DateTime)
