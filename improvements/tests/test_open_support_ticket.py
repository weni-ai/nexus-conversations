from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
import requests
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from improvements.enums import ImprovementItemStatus, ImprovementItemType, ImprovementRunStatus
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementBacklogItemConversation,
)
from improvements.services.open_support_ticket_service import (
    MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET,
    build_open_support_ticket_payload,
    open_support_ticket_for_improvement,
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
    diagnosis: str = "Instruction gap.",
    suggested_solution: dict | None = None,
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
        status=ImprovementItemStatus.ACTIVE,
    )


def _link_conversation(
    item: ImprovementBacklogItem,
    project: Project,
    *,
    contact_urn: str = "whatsapp:+5511999999999",
    contact_name: str = "Maria",
    start_date=None,
) -> Conversation:
    conversation = Conversation.objects.create(
        project=project,
        start_date=start_date or utc_datetime(2026, 2, 5, 12),
        end_date=utc_datetime(2026, 2, 5, 13),
        contact_urn=contact_urn,
        contact_name=contact_name,
    )
    ImprovementBacklogItemConversation.objects.create(
        backlog_item=item,
        conversation=conversation,
        evidence=[],
    )
    item.affected_conversations_count = item.affected_conversations.count()
    item.save(update_fields=["affected_conversations_count"])
    return conversation


@pytest.mark.django_db
class TestOpenSupportTicketService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Support Ticket Project", timezone="UTC")

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_build_payload_includes_item_and_conversations(self, mock_customization, project):
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
        conversation = _link_conversation(item, project)
        mock_customization.return_value = {
            "instructions": [{"id": 15684, "instruction": "Original text."}],
        }

        payload = build_open_support_ticket_payload(
            project.uuid,
            item.uuid,
            user_email="agent@example.com",
        )

        assert payload["project_uuid"] == str(project.uuid)
        assert payload["user_email"] == "agent@example.com"
        assert payload["improvement_item"] == {
            "uuid": str(item.uuid),
            "text": "Cancellation denied",
            "type": "wrong_behavior_due_to_instructions",
            "description": "Instruction gap.",
            "suggested_change": "Edit instruction 15684.",
            "affected_instructions": [
                {"instruction_id": 15684, "change_type": "fix", "was_changed": False},
            ],
        }
        assert payload["affected_conversations"] == [
            {
                "uuid": str(conversation.uuid),
                "contact_urn": "whatsapp:+5511999999999",
                "contact_name": "Maria",
                "started_at": "2026-02-05T12:00:00Z",
            }
        ]

    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_build_payload_limits_affected_conversations_to_ten(self, mock_customization, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        mock_customization.return_value = {"instructions": []}

        total_links = MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET + 1
        for index in range(total_links):
            _link_conversation(
                item,
                project,
                contact_urn=f"whatsapp:+55119999999{index:02d}",
                contact_name=f"Contact {index}",
                start_date=utc_datetime(2026, 2, 5, index),
            )

        payload = build_open_support_ticket_payload(
            project.uuid,
            item.uuid,
            user_email="agent@example.com",
        )

        assert len(payload["affected_conversations"]) == MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET
        assert payload["affected_conversations"][0]["contact_name"] == "Contact 0"
        assert (
            payload["affected_conversations"][-1]["contact_name"]
            == f"Contact {MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET - 1}"
        )

    @patch("improvements.services.open_support_ticket_service.NexusClient")
    @patch("improvements.services.improvements_detail_service.get_project_customization")
    def test_open_support_ticket_for_improvement_calls_nexus(
        self,
        mock_customization,
        mock_nexus_client_class,
        project,
    ):
        run = _create_run(project)
        item = _create_backlog_item(run)
        mock_customization.return_value = {"instructions": []}
        mock_client = Mock()
        mock_client.open_support_ticket.return_value = {"ticket_id": "abc-123"}
        mock_nexus_client_class.return_value = mock_client

        result = open_support_ticket_for_improvement(
            project.uuid,
            item.uuid,
            user_email="agent@example.com",
        )

        assert result == {"ticket_id": "abc-123"}
        mock_client.open_support_ticket.assert_called_once()
        call_args = mock_client.open_support_ticket.call_args
        assert call_args.args[0] == str(project.uuid)
        assert call_args.args[1]["user_email"] == "agent@example.com"
        assert call_args.args[1]["improvement_item"]["uuid"] == str(item.uuid)


@pytest.mark.django_db
class TestImprovementsOpenSupportTicketView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Support Ticket Project", timezone="UTC")

    def _url(self, project_uuid):
        return reverse(
            "project-improvements-open-support-ticket",
            kwargs={"project_uuid": project_uuid},
        )

    def _request_body(self, project, improvement_uuid):
        return {
            "improvement_uuid": str(improvement_uuid),
            "project_name": project.name,
            "email": "agent@example.com",
        }

    def test_requires_project_authorization(self, api_client, project):
        run = _create_run(project)
        item = _create_backlog_item(run)

        response = api_client.post(
            self._url(project.uuid),
            self._request_body(project, item.uuid),
            format="json",
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_404_for_missing_project(self, api_client, auth_headers):
        missing_project_uuid = uuid4()
        response = api_client.post(
            self._url(missing_project_uuid),
            {
                "improvement_uuid": str(uuid4()),
                "project_name": "Missing",
                "email": "agent@example.com",
            },
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_404_for_missing_improvement(self, api_client, auth_headers, project):
        response = api_client.post(
            self._url(project.uuid),
            self._request_body(project, uuid4()),
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("improvements.views.open_support_ticket_for_improvement")
    def test_returns_nexus_response(self, mock_open_support_ticket, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        mock_open_support_ticket.return_value = {"ticket_id": "support-123"}

        response = api_client.post(
            self._url(project.uuid),
            self._request_body(project, item.uuid),
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data == {"ticket_id": "support-123"}
        mock_open_support_ticket.assert_called_once_with(
            project.uuid,
            item.uuid,
            user_email="agent@example.com",
        )

    @patch("improvements.views.open_support_ticket_for_improvement")
    def test_returns_502_when_nexus_fails(self, mock_open_support_ticket, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(run)
        mock_response = Mock()
        mock_response.status_code = 500
        mock_open_support_ticket.side_effect = requests.HTTPError(response=mock_response)

        response = api_client.post(
            self._url(project.uuid),
            self._request_body(project, item.uuid),
            format="json",
            **auth_headers,
        )

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
