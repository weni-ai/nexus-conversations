from datetime import date
from unittest.mock import patch
from uuid import uuid4

import pendulum
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    Project,
    SubTopic,
    Topic,
)
from conversation_ms.services.project_data_transfer_service import (
    export_project_data,
    get_conversation_messages,
    import_project_data,
    resolve_export_date_range,
)


@pytest.mark.django_db
class TestProjectDataTransferService:
    @pytest.fixture
    def source_project(self):
        return Project.objects.create(name="Source", timezone="America/Sao_Paulo")

    @pytest.fixture
    def target_project(self):
        return Project.objects.create(name="Target", timezone="America/Sao_Paulo")

    @pytest.fixture
    def topic(self, source_project):
        return Topic.objects.create(name="Billing", project=source_project)

    @pytest.fixture
    def subtopic(self, topic):
        return SubTopic.objects.create(name="Invoice", topic=topic)

    def _create_conversation(
        self,
        project,
        *,
        contact_suffix="1",
        start_date=None,
        created_at=None,
        resolution=ResolutionEntities.RESOLVED,
        with_classification=None,
        with_messages=None,
    ):
        conv = Conversation.objects.create(
            project=project,
            contact_urn=f"whatsapp:+551199999999{contact_suffix}",
            channel_uuid=uuid4(),
            start_date=start_date,
            end_date=start_date,
            resolution=resolution,
        )
        if created_at is not None:
            Conversation.objects.filter(pk=conv.pk).update(created_at=created_at)
            conv.refresh_from_db()
        if with_classification:
            ConversationClassification.objects.create(
                conversation=conv,
                topic=with_classification.get("topic"),
                subtopic=with_classification.get("subtopic"),
                confidence=0.9,
            )
        if with_messages is not None:
            ConversationMessages.objects.create(conversation=conv, messages=with_messages)
        return conv

    def test_resolve_export_date_range_requires_both_dates(self, source_project):
        with pytest.raises(ValueError, match="both be provided"):
            resolve_export_date_range(source_project, date(2025, 1, 1), None)

    def test_resolve_export_date_range_rejects_inverted_range(self, source_project):
        with pytest.raises(ValueError, match="before or equal"):
            resolve_export_date_range(source_project, date(2025, 2, 1), date(2025, 1, 1))

    def test_get_conversation_messages_postgres_only(self, source_project):
        conv = self._create_conversation(
            source_project,
            with_messages=[{"text": "Hi", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )
        with patch("conversation_ms.services.project_data_transfer_service.fetch_dynamo_messages") as mock_dynamo:
            messages = get_conversation_messages(conv, include_dynamo=False)
        assert messages == [{"text": "Hi", "source": "incoming", "created_at": "2026-05-13T10:00:00"}]
        mock_dynamo.assert_not_called()

    @patch(
        "conversation_ms.services.project_data_transfer_service.fetch_dynamo_messages",
        return_value=[{"text": "Dyn", "source": "incoming", "created_at": "2026-05-13T11:00:00"}],
    )
    def test_get_conversation_messages_include_dynamo_in_progress(self, mock_dynamo, source_project):
        conv = self._create_conversation(source_project, resolution=ResolutionEntities.IN_PROGRESS)
        messages = get_conversation_messages(conv, include_dynamo=True)
        assert messages[0]["text"] == "Dyn"
        mock_dynamo.assert_called_once()

    def test_export_includes_related_entities(self, source_project, topic, subtopic):
        day = "2026-05-13"
        tz = "America/Sao_Paulo"
        start_local = pendulum.parse(day, tz=tz).start_of("day").add(hours=12).in_timezone("UTC")
        conv = self._create_conversation(
            source_project,
            start_date=start_local,
            with_classification={"topic": topic, "subtopic": subtopic},
            with_messages=[{"text": "Hello", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )

        payload = export_project_data(
            source_project.uuid,
            start_date=date(2026, 5, 13),
            end_date=date(2026, 5, 13),
        )

        assert payload["schema_version"] == 1
        assert payload["source_project"]["uuid"] == str(source_project.uuid)
        assert payload["filters"]["start_date"] == "2026-05-13"
        assert len(payload["conversations"]) == 1
        assert payload["conversations"][0]["uuid"] == str(conv.uuid)
        assert len(payload["topics"]) == 1
        assert len(payload["subtopics"]) == 1
        assert len(payload["classifications"]) == 1
        assert len(payload["conversation_messages"]) == 1

    def test_export_includes_all_project_topics_not_only_classified(self, source_project):
        topic_used = Topic.objects.create(name="Used", project=source_project)
        topic_unused = Topic.objects.create(name="Unused", project=source_project)
        SubTopic.objects.create(name="Unused sub", topic=topic_unused)

        tz = "America/Sao_Paulo"
        start_local = pendulum.parse("2026-05-13", tz=tz).start_of("day").add(hours=12).in_timezone("UTC")
        self._create_conversation(
            source_project,
            start_date=start_local,
            with_classification={"topic": topic_used, "subtopic": None},
        )

        payload = export_project_data(
            source_project.uuid,
            start_date=date(2026, 5, 13),
            end_date=date(2026, 5, 13),
        )

        topic_uuids = {row["uuid"] for row in payload["topics"]}
        assert str(topic_used.uuid) in topic_uuids
        assert str(topic_unused.uuid) in topic_uuids
        assert payload["topics"][0]["project_id"] == str(source_project.uuid)
        assert len(payload["subtopics"]) == 1
        assert payload["subtopics"][0]["topic_id"] == str(topic_unused.uuid)

    def test_import_topics_use_target_project_uuid(self, source_project, target_project):
        topic = Topic.objects.create(name="Standalone", project=source_project, description="Desc")
        subtopic = SubTopic.objects.create(name="Child", topic=topic)

        payload = export_project_data(source_project.uuid)
        import_project_data(payload, target_project.uuid, update_existing=True)

        imported_topic = Topic.objects.get(uuid=topic.uuid)
        imported_subtopic = SubTopic.objects.get(uuid=subtopic.uuid)
        assert imported_topic.project_id == target_project.uuid
        assert imported_subtopic.topic_id == topic.uuid
        assert imported_topic.name == "Standalone"
        assert Topic.objects.filter(project=source_project, uuid=topic.uuid).exists() is False

    def test_export_date_filter_excludes_out_of_range(self, source_project):
        tz = "America/Sao_Paulo"
        in_range = pendulum.parse("2026-05-13", tz=tz).start_of("day").add(hours=12).in_timezone("UTC")
        out_range = pendulum.parse("2026-05-01", tz=tz).start_of("day").in_timezone("UTC")
        self._create_conversation(source_project, contact_suffix="1", start_date=in_range)
        self._create_conversation(source_project, contact_suffix="2", start_date=out_range)

        payload = export_project_data(
            source_project.uuid,
            start_date=date(2026, 5, 13),
            end_date=date(2026, 5, 13),
        )
        assert len(payload["conversations"]) == 1
        assert payload["conversations"][0]["contact_urn"].endswith("1")

    def test_export_date_filter_includes_created_at_fallback(self, source_project):
        tz = "America/Sao_Paulo"
        created_at = pendulum.parse("2026-05-13", tz=tz).start_of("day").add(hours=8).in_timezone("UTC")
        self._create_conversation(source_project, start_date=None, created_at=created_at)

        payload = export_project_data(
            source_project.uuid,
            start_date=date(2026, 5, 13),
            end_date=date(2026, 5, 13),
        )
        assert len(payload["conversations"]) == 1

    def test_import_remaps_project_and_skip_existing(self, source_project, target_project, topic, subtopic):
        start_local = (
            pendulum.parse("2026-05-13", tz="America/Sao_Paulo").start_of("day").add(hours=12).in_timezone("UTC")
        )
        self._create_conversation(
            source_project,
            start_date=start_local,
            with_classification={"topic": topic, "subtopic": subtopic},
            with_messages=[{"text": "Hi", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )
        payload = export_project_data(source_project.uuid)

        stats = import_project_data(payload, target_project.uuid, update_existing=True)
        assert stats.created.get("conversations", 0) + stats.updated.get("conversations", 0) == 1
        assert stats.created.get("topics", 0) + stats.updated.get("topics", 0) == 1
        assert Conversation.objects.filter(project=target_project).count() == 1
        assert Topic.objects.filter(project=target_project, uuid=topic.uuid).exists()

        stats_repeat = import_project_data(payload, target_project.uuid, update_existing=False)
        assert stats_repeat.skipped["conversations"] == 1
        assert Conversation.objects.filter(project=target_project).count() == 1

    def test_import_update_existing_overwrites_messages(self, source_project, target_project):
        start_local = (
            pendulum.parse("2026-05-13", tz="America/Sao_Paulo").start_of("day").add(hours=12).in_timezone("UTC")
        )
        conv = self._create_conversation(
            source_project,
            start_date=start_local,
            with_messages=[{"text": "Original", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )
        payload = export_project_data(source_project.uuid)
        import_project_data(payload, target_project.uuid, update_existing=True)

        payload["conversation_messages"][0]["messages"] = [
            {"text": "Updated", "source": "incoming", "created_at": "2026-05-13T11:00:00"}
        ]
        stats = import_project_data(payload, target_project.uuid, update_existing=True)
        assert stats.updated["conversation_messages"] == 1

        imported_conv = Conversation.objects.get(uuid=conv.uuid)
        messages = ConversationMessages.objects.get(conversation=imported_conv).messages
        assert messages[0]["text"] == "Updated"

    def test_round_trip_preserves_data(self, source_project, target_project, topic, subtopic):
        start_local = (
            pendulum.parse("2026-05-13", tz="America/Sao_Paulo").start_of("day").add(hours=12).in_timezone("UTC")
        )
        conv = self._create_conversation(
            source_project,
            start_date=start_local,
            with_classification={"topic": topic, "subtopic": subtopic},
            with_messages=[{"text": "Round trip", "source": "incoming", "created_at": "2026-05-13T10:00:00"}],
        )
        payload = export_project_data(source_project.uuid)
        import_project_data(payload, target_project.uuid, update_existing=True)

        imported = Conversation.objects.get(uuid=conv.uuid)
        assert imported.project_id == target_project.uuid
        assert imported.contact_urn == conv.contact_urn
        assert imported.classification.topic_id == topic.uuid
        assert ConversationMessages.objects.get(conversation=imported).messages[0]["text"] == "Round trip"


@pytest.mark.django_db
class TestProjectDataTransferCommands:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="CLI Project", timezone="America/Sao_Paulo")

    def test_export_command_requires_complete_date_range(self, project, tmp_path):
        output = tmp_path / "out.json"
        with pytest.raises(CommandError, match="both be provided"):
            call_command(
                "export_project_conversations",
                project_uuid=str(project.uuid),
                start_date="2026-05-01",
                output=str(output),
            )

    def test_export_and_import_commands(self, project, tmp_path):
        tz = "America/Sao_Paulo"
        start_local = pendulum.parse("2026-05-13", tz=tz).start_of("day").add(hours=12).in_timezone("UTC")
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511888888888",
            channel_uuid=uuid4(),
            start_date=start_local,
            end_date=start_local,
        )
        target = Project.objects.create(name="Target CLI", timezone=tz)
        output = tmp_path / "export.json"

        call_command(
            "export_project_conversations",
            project_uuid=str(project.uuid),
            start_date="2026-05-13",
            end_date="2026-05-13",
            stdout_summary=True,
            output=str(output),
        )
        assert output.is_file()

        call_command(
            "import_project_conversations",
            input=str(output),
            target_project_uuid=str(target.uuid),
            update_existing=True,
        )
        assert Conversation.objects.filter(project=target).count() == 1
