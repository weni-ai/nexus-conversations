from datetime import datetime
from datetime import timezone as dt_tz
from unittest.mock import patch
from uuid import uuid4

import pytest

from conversation_ms.models import Conversation, Project


@pytest.mark.django_db
class TestConversationCountService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Count Project", timezone="America/Sao_Paulo")

    @patch("improvements.adapters.boto3.get_boto3_client")
    def test_select_random_conversations_caps_at_available_total(self, mock_get_client, project):
        from improvements.services.conversation_count_service import select_random_conversations_in_range

        Conversation.objects.create(
            project=project,
            start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
            end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
        )

        selected = select_random_conversations_in_range(
            project.uuid,
            "2026-02-05T00:00:00.000000Z",
            "2026-02-05T23:59:59.000000Z",
            10,
        )

        assert len(selected) == 1
        mock_get_client.assert_not_called()

    def test_iter_conversation_batches_preserves_uuid_order(self, project):
        from improvements.services.conversation_count_service import iter_conversation_batches_by_uuids

        uuids = []
        for _ in range(5):
            conversation = Conversation.objects.create(
                project=project,
                start_date=datetime(2026, 2, 5, 12, 0, 0, tzinfo=dt_tz.utc),
                end_date=datetime(2026, 2, 5, 13, 0, 0, tzinfo=dt_tz.utc),
                channel_uuid=uuid4(),
            )
            uuids.append(conversation.uuid)

        batches = list(iter_conversation_batches_by_uuids(uuids, batch_size=2))

        assert len(batches) == 3
        flattened = [conversation.uuid for batch in batches for conversation in batch]
        assert flattened == uuids
