from unittest.mock import patch
from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, ConversationMessages, Project
from improvements.enums import (
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementBacklogItemConversation,
    ImprovementRunConversation,
)
from improvements.services.improvement_message_service import (
    filter_messages_by_uuids,
    load_conversation_messages,
    map_affected_conversation,
)
from improvements.services.improvements_affected_conversations_service import list_affected_conversations
from improvements.services.improvements_detail_service import (
    ImprovementDetailNotFound,
    get_improvement_detail,
)
from improvements.utils.time import utc_datetime


def _create_run(project: Project) -> ImprovementAnalysisRun:
    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date="2026-02-05",
        triggered_on_date="2026-02-06",
        status=ImprovementRunStatus.COMPLETED,
        range_start_utc=utc_datetime(2026, 2, 5),
        range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
    )


def _create_backlog_item(
    run: ImprovementAnalysisRun,
    *,
    dimension_id: str = "wrong_behavior_due_to_instructions",
    title: str = "Cancellation denied",
    item_status: str = ImprovementItemStatus.ACTIVE,
    suggested_solution: dict | None = None,
    diagnosis: str = "Instruction gap.",
) -> ImprovementBacklogItem:
    return ImprovementBacklogItem.objects.create(
        project=run.project,
        run=run,
        dimension_id=dimension_id,
        item_type=ImprovementItemType.BEHAVIOR,
        title=title,
        diagnosis=diagnosis,
        suggested_solution=suggested_solution or {},
        affected_conversations_count=0,
        status=item_status,
    )


def _link_conversation(
    item: ImprovementBacklogItem,
    project: Project,
    *,
    contact_urn: str = "whatsapp:+5511999999999",
    contact_name: str = "Maria",
    evidence: list | None = None,
) -> Conversation:
    conversation = Conversation.objects.create(
        project=project,
        start_date=utc_datetime(2026, 2, 5, 12),
        end_date=utc_datetime(2026, 2, 5, 13),
        contact_urn=contact_urn,
        contact_name=contact_name,
    )
    ImprovementBacklogItemConversation.objects.create(
        backlog_item=item,
        conversation=conversation,
        evidence=evidence or [],
    )
    item.affected_conversations_count = item.affected_conversations.count()
    item.save(update_fields=["affected_conversations_count"])
    return conversation


@pytest.mark.django_db
class TestImprovementsDetailService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Detail Project", timezone="UTC")

    @pytest.fixture
    def backlog_item(self, project):
        run = _create_run(project)
        return _create_backlog_item(
            run,
            suggested_solution={
                "target": "manager_instruction",
                "suggested_change": "Edit instruction 15684.",
                "details": {
                    "instruction_change_type": "fix",
                    "affected_instruction_ids": [15684],
                },
            },
        )

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_returns_detail_payload_without_conversations(self, mock_customization, project, backlog_item):
        _link_conversation(backlog_item, project)
        mock_customization.return_value = {
            "instructions": [{"id": 15684, "instruction": "Original text."}],
        }

        result = get_improvement_detail(project.uuid, backlog_item.uuid)

        assert result == {
            "uuid": str(backlog_item.uuid),
            "text": "Cancellation denied",
            "type": "wrong_behavior_due_to_instructions",
            "description": "Instruction gap.",
            "suggested_change": "Edit instruction 15684.",
            "status": "pending",
            "affected_instructions": [
                {"instruction_id": 15684, "change_type": "fix", "was_changed": False},
            ],
        }
        assert "conversations" not in result

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    @pytest.mark.parametrize(
        ("db_status", "api_status"),
        [
            (ImprovementItemStatus.ACTIVE, "pending"),
            (ImprovementItemStatus.IGNORED, "ignored"),
            (ImprovementItemStatus.RESOLVED, "resolved"),
        ],
    )
    def test_maps_item_status(self, mock_customization, project, db_status, api_status):
        run = _create_run(project)
        item = _create_backlog_item(run, item_status=db_status)
        mock_customization.return_value = {"instructions": []}

        result = get_improvement_detail(project.uuid, item.uuid)

        assert result["status"] == api_status

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_maps_custom_monitor_type(self, mock_customization, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            dimension_id=f"custom:{uuid4()}",
            title="Custom issue",
        )
        mock_customization.return_value = {"instructions": []}

        result = get_improvement_detail(project.uuid, item.uuid)

        assert result["type"] == "custom_analysis"

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_remove_instruction_was_changed_when_missing_in_nexus(self, mock_customization, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            suggested_solution={
                "target": "manager_instruction",
                "details": {
                    "instruction_change_type": "remove",
                    "affected_instruction_ids": [99],
                },
            },
        )
        mock_customization.return_value = {"instructions": [{"id": 1, "instruction": "Keep me"}]}

        result = get_improvement_detail(project.uuid, item.uuid)

        assert result["affected_instructions"] == [
            {"instruction_id": 99, "change_type": "remove", "was_changed": True},
        ]

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_fix_with_snapshot_detects_text_change(self, mock_customization, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            suggested_solution={
                "kind": "instruction_edit",
                "instruction_refs": [
                    {"instruction_id": 42, "snapshot_text": "Old policy text"},
                ],
            },
        )
        mock_customization.return_value = {
            "instructions": [{"id": 42, "instruction": "Updated policy text"}],
        }

        result = get_improvement_detail(project.uuid, item.uuid)

        assert result["affected_instructions"] == [
            {"instruction_id": 42, "change_type": "fix", "was_changed": True},
        ]

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_nexus_unavailable_sets_was_changed_null(self, mock_customization, project, backlog_item):
        mock_customization.side_effect = RuntimeError("Nexus unavailable")

        result = get_improvement_detail(project.uuid, backlog_item.uuid)

        assert result["affected_instructions"] == [
            {"instruction_id": 15684, "change_type": "fix", "was_changed": None},
        ]

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_non_instruction_problem_type_returns_empty_instructions(self, mock_customization, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            dimension_id="missing_static_knowledge",
            suggested_solution={
                "target": "knowledge_base",
                "details": {"knowledge_topic": "return policy"},
            },
        )
        mock_customization.return_value = {"instructions": [{"id": 1, "instruction": "X"}]}

        result = get_improvement_detail(project.uuid, item.uuid)

        assert result["type"] == "missing_static_knowledge"
        assert result["affected_instructions"] == []

    def test_raises_not_found_for_superseded_item(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run, item_status=ImprovementItemStatus.SUPERSEDED)

        with pytest.raises(ImprovementDetailNotFound):
            get_improvement_detail(project.uuid, item.uuid)

    def test_raises_not_found_for_missing_item(self, project):
        with pytest.raises(ImprovementDetailNotFound):
            get_improvement_detail(project.uuid, uuid4())


@pytest.mark.django_db
class TestImprovementMessageService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Detail Project", timezone="UTC")

    def test_filter_messages_by_uuids_preserves_evidence_order(self):
        messages = [
            {"uuid": "b", "id": "b", "text": "B", "source": "incoming", "created_at": "t2"},
            {"uuid": "a", "id": "a", "text": "A", "source": "outgoing", "created_at": "t1"},
        ]

        filtered = filter_messages_by_uuids(messages, ["a", "b"])

        assert [message["uuid"] for message in filtered] == ["a", "b"]

    def test_map_affected_conversation_hydrates_messages_from_postgres(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        conversation = _link_conversation(
            item,
            project,
            evidence=["msg-003-ccc", "msg-004-ddd"],
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "message_id": "msg-003-ccc",
                    "text": "First",
                    "source": "user",
                    "created_at": "2026-06-23T09:44:26-03:00",
                },
                {
                    "message_id": "msg-004-ddd",
                    "text": "Second",
                    "source": "agent",
                    "created_at": "2026-06-23T09:45:26-03:00",
                },
            ],
        )
        link = item.affected_conversations.select_related("conversation").first()

        result = map_affected_conversation(item, link)

        assert result["uuid"] == str(conversation.uuid)
        assert result["messages"] == [
            {
                "uuid": "msg-003-ccc",
                "id": "msg-003-ccc",
                "text": "First",
                "source": "incoming",
                "created_at": "2026-06-23T09:44:26-03:00",
            },
            {
                "uuid": "msg-004-ddd",
                "id": "msg-004-ddd",
                "text": "Second",
                "source": "outgoing",
                "created_at": "2026-06-23T09:45:26-03:00",
            },
        ]

    def test_falls_back_to_dimension_results_message_uuids(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        conversation = _link_conversation(item, project)
        ImprovementRunConversation.objects.create(
            run=item.run,
            conversation=conversation,
            dimension_results=[
                {
                    "dimension_id": "wrong_behavior_due_to_instructions",
                    "message_uuids_relevant": ["msg-fallback-1"],
                }
            ],
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "message_id": "msg-fallback-1",
                    "text": "Fallback",
                    "source": "user",
                    "created_at": "2026-06-23T09:44:26-03:00",
                },
            ],
        )
        link = item.affected_conversations.select_related("conversation").first()

        result = map_affected_conversation(item, link)

        assert result["messages"][0]["uuid"] == "msg-fallback-1"

    def test_load_conversation_messages_returns_empty_when_unavailable(self, project):
        conversation = Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 2, 5, 12),
            end_date=utc_datetime(2026, 2, 5, 13),
            contact_urn="whatsapp:+5511888888888",
        )

        assert load_conversation_messages(conversation) == []


@pytest.mark.django_db
class TestImprovementsAffectedConversationsService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Detail Project", timezone="UTC")

    def test_paginates_affected_conversations(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        for index in range(3):
            _link_conversation(
                item,
                project,
                contact_urn=f"whatsapp:+551199999999{index}",
                contact_name=f"Contact {index}",
            )

        page_one = list_affected_conversations(
            project.uuid,
            item.uuid,
            page=1,
            page_size=2,
            base_url="http://testserver/api/v1/projects/x/improvements/y/affected_conversations/",
        )
        page_two = list_affected_conversations(
            project.uuid,
            item.uuid,
            page=2,
            page_size=2,
            base_url="http://testserver/api/v1/projects/x/improvements/y/affected_conversations/",
        )

        assert page_one["count"] == 3
        assert len(page_one["results"]) == 2
        assert page_one["next"] is not None
        assert page_one["previous"] is None
        assert len(page_two["results"]) == 1
        assert page_two["previous"] is not None
        assert page_two["next"] is None


@pytest.mark.django_db
class TestProjectImprovementDetailView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Detail Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def _detail_url(self, project_uuid, improvement_uuid):
        return reverse(
            "project-improvement-detail",
            kwargs={"project_uuid": project_uuid, "improvement_uuid": improvement_uuid},
        )

    def test_requires_auth(self, api_client, project):
        run = _create_run(project)
        item = _create_backlog_item(run)

        response = api_client.get(self._detail_url(project.uuid, item.uuid))

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_404_for_missing_project(self, api_client, auth_headers):
        response = api_client.get(self._detail_url(uuid4(), uuid4()), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_404_for_missing_item(self, api_client, auth_headers, project):
        response = api_client.get(self._detail_url(project.uuid, uuid4()), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_404_for_superseded_item(self, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(run, item_status=ImprovementItemStatus.SUPERSEDED)

        response = api_client.get(self._detail_url(project.uuid, item.uuid), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_returns_detail_payload(self, mock_customization, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            suggested_solution={
                "target": "manager_instruction",
                "suggested_change": "Edit instruction 15684.",
                "details": {
                    "instruction_change_type": "fix",
                    "affected_instruction_ids": [15684],
                },
            },
        )
        mock_customization.return_value = {
            "instructions": [{"id": 15684, "instruction": "Same text"}],
        }

        response = api_client.get(self._detail_url(project.uuid, item.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data == {
            "uuid": str(item.uuid),
            "text": "Cancellation denied",
            "type": "wrong_behavior_due_to_instructions",
            "description": "Instruction gap.",
            "suggested_change": "Edit instruction 15684.",
            "status": "pending",
            "affected_instructions": [
                {
                    "instruction_id": 15684,
                    "change_type": "fix",
                    "was_changed": False,
                }
            ],
        }


@pytest.mark.django_db
class TestProjectImprovementAffectedConversationsView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Detail Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def _url(self, project_uuid, improvement_uuid):
        return reverse(
            "project-improvement-affected-conversations",
            kwargs={"project_uuid": project_uuid, "improvement_uuid": improvement_uuid},
        )

    def test_requires_auth(self, api_client, project):
        run = _create_run(project)
        item = _create_backlog_item(run)

        response = api_client.get(self._url(project.uuid, item.uuid))

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_404_for_missing_item(self, api_client, auth_headers, project):
        response = api_client.get(self._url(project.uuid, uuid4()), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_paginated_affected_conversations(self, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        conversation = _link_conversation(
            item,
            project,
            evidence=["msg-003-ccc", "msg-004-ddd"],
        )
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "message_id": "msg-003-ccc",
                    "text": "First",
                    "source": "user",
                    "created_at": "2026-06-23T09:44:26-03:00",
                },
                {
                    "message_id": "msg-004-ddd",
                    "text": "Second",
                    "source": "agent",
                    "created_at": "2026-06-23T09:45:26-03:00",
                },
            ],
        )

        response = api_client.get(self._url(project.uuid, item.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["count"] == 1
        assert response.data["previous"] is None
        assert response.data["next"] is None
        assert response.data["results"] == [
            {
                "uuid": str(conversation.uuid),
                "contact_urn": "whatsapp:+5511999999999",
                "contact_name": "Maria",
                "messages": [
                    {
                        "uuid": "msg-003-ccc",
                        "id": "msg-003-ccc",
                        "text": "First",
                        "source": "incoming",
                        "created_at": "2026-06-23T09:44:26-03:00",
                    },
                    {
                        "uuid": "msg-004-ddd",
                        "id": "msg-004-ddd",
                        "text": "Second",
                        "source": "outgoing",
                        "created_at": "2026-06-23T09:45:26-03:00",
                    },
                ],
            }
        ]

    def test_supports_custom_analysis_item(self, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(run, dimension_id=f"custom:{uuid4()}")
        conversation = _link_conversation(item, project)
        ConversationMessages.objects.create(
            conversation=conversation,
            messages=[
                {
                    "message_id": "msg-custom",
                    "text": "Custom",
                    "source": "user",
                    "created_at": "2026-06-23T09:44:26-03:00",
                },
            ],
        )
        link = item.affected_conversations.first()
        link.evidence = ["msg-custom"]
        link.save(update_fields=["evidence"])

        response = api_client.get(self._url(project.uuid, item.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["results"][0]["messages"][0]["uuid"] == "msg-custom"
