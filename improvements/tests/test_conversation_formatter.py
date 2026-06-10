from datetime import datetime
from datetime import timezone as dt_tz
from unittest.mock import patch

import pytest

from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    Project,
    SubTopic,
    Topic,
)
from improvements.services.conversation_formatter import (
    build_all_messages,
    build_conversation_detail,
    build_listing_item,
    build_raw_conversation,
    build_raw_conversations,
    get_traces_by_message_id,
)


@pytest.mark.django_db
class TestConversationFormatter:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Formatter Project")

    @pytest.fixture
    def conversation(self, project):
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="ext:e2ef1b3e-e9b8-4cd8-88a0-f105e1edc0df",
            contact_name="Cliente Teste",
            resolution="0",
            start_date=datetime(2026, 5, 29, 15, 29, 33, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 5, 30, 2, 59, 59, tzinfo=dt_tz.utc),
        )
        topic = Topic.objects.create(name="SITE/APP", project=project)
        subtopic = SubTopic.objects.create(name="FINALIZAÇÃO DO PEDIDO", topic=topic)
        ConversationClassification.objects.create(
            conversation=conversation,
            topic=topic,
            subtopic=subtopic,
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "uuid": "5cb6bf30-1904-49c7-b192-e8a67e0dcba0",
                    "text": "Finalizar um pedido",
                    "source": "incoming",
                    "created_at": "2026-05-29T15:29:33",
                },
                {
                    "uuid": "c80dec64-c98b-41d6-b2a7-b2420d090dc3",
                    "text": "Oi, eu sou o assistente virtual da C&A ...",
                    "source": "outgoing",
                    "created_at": "2026-05-29T15:29:43",
                },
            ],
        )
        return Conversation.objects.select_related(
            "messages_data",
            "classification",
            "classification__topic",
            "classification__subtopic",
        ).get(uuid=conversation.uuid)

    def test_build_conversation_detail(self, conversation):
        detail = build_conversation_detail(conversation)

        assert detail["conversation_uuid"] == str(conversation.uuid)
        assert detail["contact_urn"] == "ext:e2ef1b3e-e9b8-4cd8-88a0-f105e1edc0df"
        assert detail["status"] == "Resolved"
        assert detail["topic"] == "SITE/APP"
        assert detail["created_at"].startswith("2026-05-29T15:29:33")
        assert detail["ended_at"].startswith("2026-05-30T02:59:59")

    def test_build_listing_item(self, conversation):
        listing_item = build_listing_item(conversation)

        assert listing_item["uuid"] == str(conversation.uuid)
        assert listing_item["contact_name"] == "Cliente Teste"
        assert listing_item["status"] == "Resolved"
        assert listing_item["resolution"] == 0
        assert listing_item["classification"] == {
            "topic": "SITE/APP",
            "subtopic": "FINALIZAÇÃO DO PEDIDO",
        }

    def test_build_all_messages(self, conversation):
        messages = build_all_messages(conversation)

        assert messages == [
            {
                "uuid": "5cb6bf30-1904-49c7-b192-e8a67e0dcba0",
                "id": "5cb6bf30-1904-49c7-b192-e8a67e0dcba0",
                "created_at": "2026-05-29T15:29:33",
                "source": "incoming",
                "text": "Finalizar um pedido",
            },
            {
                "uuid": "c80dec64-c98b-41d6-b2a7-b2420d090dc3",
                "id": "c80dec64-c98b-41d6-b2a7-b2420d090dc3",
                "created_at": "2026-05-29T15:29:43",
                "source": "outgoing",
                "text": "Oi, eu sou o assistente virtual da C&A ...",
            },
        ]

    def test_get_traces_by_message_id_fetches_outgoing_messages(self, conversation):
        with patch("improvements.services.conversation_formatter.fetch_agent_traces") as mock_fetch:
            mock_fetch.return_value = [
                {
                    "trace": {
                        "config": {"type": "executing_tool", "toolName": "contact-fields"},
                        "trace": {"orchestrationTrace": {}},
                    }
                }
            ]

            traces = get_traces_by_message_id(conversation)

        assert traces == {
            "c80dec64-c98b-41d6-b2a7-b2420d090dc3": [
                {
                    "trace": {
                        "config": {"type": "executing_tool", "toolName": "contact-fields"},
                        "trace": {"orchestrationTrace": {}},
                    }
                }
            ]
        }
        mock_fetch.assert_called_once_with(
            str(conversation.project.uuid),
            "c80dec64-c98b-41d6-b2a7-b2420d090dc3",
        )

    def test_build_raw_conversation(self, conversation):
        with patch("improvements.services.conversation_formatter.fetch_agent_traces") as mock_fetch:
            mock_fetch.return_value = [{"trace": {"config": {}, "trace": {}}}]
            raw_conversation = build_raw_conversation(conversation)

        assert set(raw_conversation.keys()) == {
            "detail",
            "listing_item",
            "all_messages",
            "traces_by_message_id",
        }
        assert raw_conversation["detail"]["conversation_uuid"] == str(conversation.uuid)
        assert len(raw_conversation["all_messages"]) == 2
        assert raw_conversation["traces_by_message_id"]["c80dec64-c98b-41d6-b2a7-b2420d090dc3"]

    def test_build_raw_conversations_wraps_list(self, conversation):
        payload = build_raw_conversations([conversation])

        assert len(payload["raw_conversations"]) == 1
        assert payload["raw_conversations"][0]["detail"]["conversation_uuid"] == str(conversation.uuid)
