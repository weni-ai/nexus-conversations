"""Unit tests for conversation message window filtering (detail serializer)."""

from types import SimpleNamespace

import pendulum
import pytest

from conversation_ms.serializers import _filter_messages_by_conversation_window


def _conversation_stub(*, start_date=None, end_date=None):
    return SimpleNamespace(start_date=start_date, end_date=end_date)


class TestFilterMessagesByConversationWindow:
    def test_keeps_first_message_when_start_date_has_subseconds_and_message_is_second_precision(self):
        """
        Dynamo stores created_at at second resolution; conversation.start_date can include
        subseconds from the same first event. The first line must not be dropped.
        """
        conversation = _conversation_stub(
            start_date=pendulum.parse("2026-05-02T15:23:41.660972Z").in_timezone("UTC"),
            end_date=pendulum.parse("2026-05-03T23:59:59.999999Z").in_timezone("UTC"),
        )
        messages = [
            {"text": "first incoming", "created_at": "2026-05-02T15:23:41"},
            {"text": "outgoing +3s", "created_at": "2026-05-02T15:23:44"},
        ]

        filtered = _filter_messages_by_conversation_window(messages, conversation)

        assert [m["text"] for m in filtered] == ["first incoming", "outgoing +3s"]

    def test_excludes_messages_strictly_before_window_start_second(self):
        conversation = _conversation_stub(
            start_date=pendulum.parse("2026-05-02T15:23:41.660972Z").in_timezone("UTC"),
            end_date=pendulum.parse("2026-05-03T23:59:59.999999Z").in_timezone("UTC"),
        )
        messages = [
            {"text": "too early", "created_at": "2026-05-02T15:23:40"},
            {"text": "ok same second as floor", "created_at": "2026-05-02T15:23:41"},
        ]

        filtered = _filter_messages_by_conversation_window(messages, conversation)

        assert [m["text"] for m in filtered] == ["ok same second as floor"]

    def test_no_window_returns_all(self):
        conversation = _conversation_stub(start_date=None, end_date=None)
        messages = [{"text": "a", "created_at": "2020-01-01T00:00:00Z"}]

        assert _filter_messages_by_conversation_window(messages, conversation) == messages

    @pytest.mark.django_db
    def test_filter_runs_on_real_conversation_model_instance(self):
        """Regression: function accepts real Conversation ORM instances (detail path)."""
        from conversation_ms.models import Conversation, Project

        project = Project.objects.create(name="Window ORM")
        conversation = Conversation.objects.create(
            project=project,
            resolution="2",
            start_date="2026-06-01T10:00:00.123456Z",
            end_date="2026-06-01T18:00:00Z",
        )
        messages = [{"text": "edge", "created_at": "2026-06-01T10:00:00"}]

        filtered = _filter_messages_by_conversation_window(messages, conversation)

        assert len(filtered) == 1
        assert filtered[0]["text"] == "edge"
