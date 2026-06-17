from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from improvements.enums import (
    ImprovementConversationProcessingStatus,
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementRunConversation,
)
from improvements.services.improvements_list_service import list_project_improvements
from improvements.utils.time import utc_datetime


def _create_run(project: Project, *, status: str = ImprovementRunStatus.COMPLETED) -> ImprovementAnalysisRun:
    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date="2026-02-05",
        triggered_on_date="2026-02-06",
        status=status,
        sample_size=2,
        conversations_total=2,
        range_start_utc=utc_datetime(2026, 2, 5),
        range_end_utc=utc_datetime(2026, 2, 5, 23, 59, 59),
    )


def _create_backlog_item(
    run: ImprovementAnalysisRun,
    *,
    dimension_id: str = "missing_static_knowledge",
    title: str = "Missing policy info",
    affected_count: int = 2,
    status: str = ImprovementItemStatus.ACTIVE,
) -> ImprovementBacklogItem:
    return ImprovementBacklogItem.objects.create(
        project=run.project,
        run=run,
        dimension_id=dimension_id,
        item_type=ImprovementItemType.KNOWLEDGE,
        title=title,
        diagnosis="The agent did not mention the return policy.",
        affected_conversations_count=affected_count,
        status=status,
    )


@pytest.mark.django_db
class TestImprovementsListService:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="List Project", timezone="UTC")

    def test_returns_empty_when_no_data(self, project):
        result = list_project_improvements(project.uuid)

        assert result == {"improvements_count": 0, "improvements": []}

    def test_returns_active_backlog_items(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run)

        result = list_project_improvements(project.uuid)

        assert result["improvements_count"] == 1
        assert result["improvements"] == [
            {
                "uuid": str(item.uuid),
                "text": "Missing policy info",
                "type": "missing_static_knowledge",
                "conversations_count": 2,
            }
        ]

    def test_excludes_superseded_backlog_items(self, project):
        run = _create_run(project)
        _create_backlog_item(run, status=ImprovementItemStatus.SUPERSEDED)

        result = list_project_improvements(project.uuid)

        assert result["improvements_count"] == 0

    def test_excludes_custom_monitor_backlog_items(self, project):
        run = _create_run(project)
        ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id=f"custom:{uuid4()}",
            item_type=ImprovementItemType.CUSTOM,
            title="Custom monitor issue",
            diagnosis="Custom diagnosis",
            affected_conversations_count=1,
            status=ImprovementItemStatus.ACTIVE,
        )

        result = list_project_improvements(project.uuid)

        assert result["improvements_count"] == 0

    def test_appends_amazing_conversation_entry(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run, affected_count=1)
        conversation = Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 2, 5, 12),
            end_date=utc_datetime(2026, 2, 5, 13),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=conversation,
            processing_status=ImprovementConversationProcessingStatus.COMPLETED,
            is_amazing_conversation=True,
        )

        result = list_project_improvements(project.uuid)

        assert result["improvements_count"] == 2
        assert result["improvements"][0]["uuid"] == str(item.uuid)
        assert result["improvements"][1] == {
            "uuid": str(run.uuid),
            "text": "Amazing conversation",
            "type": "amazing_conversation",
            "conversations_count": 1,
        }

    def test_returns_only_amazing_when_no_backlog(self, project):
        run = _create_run(project)
        conversation = Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 2, 5, 12),
            end_date=utc_datetime(2026, 2, 5, 13),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=conversation,
            processing_status=ImprovementConversationProcessingStatus.COMPLETED,
            is_amazing_conversation=True,
        )

        result = list_project_improvements(project.uuid)

        assert result["improvements_count"] == 1
        assert result["improvements"][0]["type"] == "amazing_conversation"
        assert result["improvements"][0]["conversations_count"] == 1


@pytest.mark.django_db
class TestProjectImprovementsListView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="List Project", timezone="UTC")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def _url(self, project_uuid):
        return reverse("project-improvements-list", kwargs={"project_uuid": project_uuid})

    def test_requires_auth(self, api_client, project):
        response = api_client.get(self._url(project.uuid))

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_returns_404_for_missing_project(self, api_client, auth_headers):
        response = api_client.get(self._url(uuid4()), **auth_headers)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_returns_empty_list(self, api_client, auth_headers, project):
        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data == {"improvements_count": 0, "improvements": []}

    def test_returns_active_backlog_item(self, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            dimension_id="instruction_non_compliance",
            title="Skipped instruction",
            affected_count=3,
        )

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements_count"] == 1
        assert response.data["improvements"][0] == {
            "uuid": str(item.uuid),
            "text": "Skipped instruction",
            "type": "instruction_non_compliance",
            "conversations_count": 3,
        }

    def test_excludes_superseded_backlog_via_api(self, api_client, auth_headers, project):
        run = _create_run(project)
        _create_backlog_item(run, status=ImprovementItemStatus.SUPERSEDED)

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements_count"] == 0

    def test_returns_amazing_conversation_via_api(self, api_client, auth_headers, project):
        run = _create_run(project)
        conversation = Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 2, 5, 12),
            end_date=utc_datetime(2026, 2, 5, 13),
        )
        ImprovementRunConversation.objects.create(
            run=run,
            conversation=conversation,
            processing_status=ImprovementConversationProcessingStatus.COMPLETED,
            is_amazing_conversation=True,
        )

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements_count"] == 1
        assert response.data["improvements"][0]["type"] == "amazing_conversation"
        assert response.data["improvements"][0]["text"] == "Amazing conversation"
        assert response.data["improvements"][0]["conversations_count"] == 1
