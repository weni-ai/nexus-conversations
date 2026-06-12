from datetime import date
from unittest.mock import MagicMock, patch

import pendulum
import pytest

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation, ConversationMessages, Project
from conversation_ms.services.conversation_csv_export_service import (
    CSV_HEADERS,
    export_conversations_csv_bytes,
    format_msgs_cell,
    format_reason_cell,
    resolve_target_date,
)


@pytest.mark.django_db
class TestConversationCsvExportService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="TokStok", timezone="America/Sao_Paulo")

    def test_resolve_target_date_invalid_timezone_uses_fallback(self):
        project = Project.objects.create(name="Bad TZ", timezone="Invalid/X")
        with patch("conversation_ms.services.conversation_csv_export_service.pendulum") as mock_pendulum:
            mock_now = MagicMock()
            mock_now.format.return_value = "2026-05-20"
            mock_pendulum.now.return_value = mock_now
            assert resolve_target_date(project, None) == "2026-05-20"
            mock_pendulum.now.assert_called_once()
            tz_arg = mock_pendulum.now.call_args[0][0]
            assert tz_arg != "Invalid/X"

    def test_resolve_target_date_explicit(self, project):
        assert resolve_target_date(project, "2026-05-13") == "2026-05-13"
        assert resolve_target_date(project, date(2026, 5, 13)) == "2026-05-13"

    @patch("conversation_ms.services.conversation_csv_export_service.pendulum")
    def test_resolve_target_date_defaults_to_today(self, mock_pendulum, project):
        mock_now = MagicMock()
        mock_now.format.return_value = "2026-05-20"
        mock_pendulum.now.return_value = mock_now
        assert resolve_target_date(project, None) == "2026-05-20"
        mock_pendulum.now.assert_called_once_with("America/Sao_Paulo")

    def test_format_reason_cell_without_classification(self, project):
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
        )
        assert format_reason_cell(conv) == ""

    @patch("conversation_ms.services.project_data_transfer_service.fetch_dynamo_messages")
    def test_format_msgs_cell_resolved_skips_dynamo_when_postgres_has_messages(self, mock_dynamo, project):
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
            resolution=ResolutionEntities.RESOLVED,
        )
        from conversation_ms.models import ConversationMessages

        ConversationMessages.objects.create(
            conversation=conv,
            messages=[{"text": "Oi", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )
        cell = format_msgs_cell(conv)
        assert "i:Oi" in cell
        mock_dynamo.assert_not_called()

    def test_format_msgs_cell_from_postgres(self, project):
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
            resolution=ResolutionEntities.RESOLVED,
        )
        ConversationMessages.objects.create(
            conversation=conv,
            messages=[
                {"text": "Oi", "source": "incoming", "created_at": "2026-05-13T10:00:00"},
                {"text": "Olá", "source": "outgoing", "created_at": "2026-05-13T10:01:00"},
            ],
        )
        cell = format_msgs_cell(conv)
        assert "i:Oi" in cell
        assert "o:Olá" in cell

    @patch(
        "conversation_ms.services.project_data_transfer_service.fetch_dynamo_messages",
        return_value=[{"text": "From dynamo", "source": "incoming", "created_at": "2026-05-13T11:00:00"}],
    )
    def test_format_msgs_cell_in_progress_prefers_dynamo(self, mock_dynamo, project):
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
            resolution=ResolutionEntities.IN_PROGRESS,
        )
        cell = format_msgs_cell(conv)
        assert "i:From dynamo" in cell
        mock_dynamo.assert_called_once()

    def test_export_conversations_csv_bytes_filters_by_day(self, project):
        day = "2026-05-13"
        tz = "America/Sao_Paulo"
        start_local = pendulum.parse(day, tz=tz).start_of("day")
        end_local = start_local.end_of("day")
        mid = start_local.add(hours=12).in_timezone("UTC")

        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511111111111",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
            start_date=mid,
            end_date=end_local.in_timezone("UTC"),
        )
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5522222222222",
            channel_uuid="afa615d7-932a-4803-b3ba-00c069e602b2",
            start_date=pendulum.parse("2026-05-01", tz=tz).in_timezone("UTC"),
        )

        body, row_count, resolved_day = export_conversations_csv_bytes(str(project.uuid), target_date=day)
        text = body.decode("utf-8")
        assert resolved_day == day
        assert row_count == 1
        assert "conversation_uuid" in text.splitlines()[0]
        assert text.splitlines()[0] == ",".join(CSV_HEADERS)
        assert "+5511111111111" in text
        assert "+5522222222222" not in text
