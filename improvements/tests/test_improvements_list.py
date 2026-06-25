from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from freezegun import freeze_time
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from conversation_ms.utils.date_helpers import ProjectDay
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
from improvements.services.improvements_list_service import (
    IDLE_IMPROVEMENTS_TASK,
    list_project_improvements,
)
from improvements.utils.time import utc_datetime


def _empty_list_payload(*, yesterday_conversations_count: int = 0) -> dict:
    return {
        "yesterday_conversations_count": yesterday_conversations_count,
        "improvements_task": IDLE_IMPROVEMENTS_TASK,
        "improvements": [],
    }


def _create_run(
    project: Project,
    *,
    status: str = ImprovementRunStatus.COMPLETED,
    conversations_processed: int = 0,
    conversations_total: int = 2,
    sample_size: int = 2,
) -> ImprovementAnalysisRun:
    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date="2026-02-05",
        triggered_on_date="2026-02-06",
        status=status,
        sample_size=sample_size,
        conversations_total=conversations_total,
        conversations_processed=conversations_processed,
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
        result = list_project_improvements(project)

        assert result == _empty_list_payload()

    @freeze_time("2026-06-03T15:00:00Z")
    def test_returns_yesterday_conversations_count(self, project):
        yesterday = ProjectDay.for_yesterday(project.timezone)
        start_utc, end_utc = yesterday.get_utc_range()
        Conversation.objects.create(
            project=project,
            start_date=utc_datetime(
                start_utc.year,
                start_utc.month,
                start_utc.day,
                start_utc.hour,
            ),
            end_date=utc_datetime(
                end_utc.year,
                end_utc.month,
                end_utc.day,
                end_utc.hour,
            ),
        )
        Conversation.objects.create(
            project=project,
            start_date=utc_datetime(2026, 6, 1, 12),
            end_date=utc_datetime(2026, 6, 1, 13),
        )

        result = list_project_improvements(project)

        assert result["yesterday_conversations_count"] == 1
        assert result["improvements_task"] == IDLE_IMPROVEMENTS_TASK
        assert result["improvements"] == []

    def test_returns_active_backlog_items(self, project):
        run = _create_run(project)
        item = _create_backlog_item(run)

        result = list_project_improvements(project)

        assert len(result["improvements"]) == 1
        assert result["improvements"] == [
            {
                "uuid": str(item.uuid),
                "text": "Missing policy info",
                "type": "missing_static_knowledge",
                "conversations_count": 2,
            }
        ]
        assert result["improvements_task"]["is_running"] is False
        assert result["improvements_task"]["progress"] == 0
        assert result["improvements_task"]["total"] == 2

    def test_returns_running_improvements_task(self, project):
        run = _create_run(
            project,
            status=ImprovementRunStatus.IN_PROGRESS,
            conversations_processed=3,
            conversations_total=5,
        )

        result = list_project_improvements(project)

        assert result["improvements_task"] == {
            "is_running": True,
            "progress": 3,
            "total": 5,
            "created_at": run.started_at,
        }

    def test_excludes_superseded_backlog_items(self, project):
        run = _create_run(project)
        _create_backlog_item(run, status=ImprovementItemStatus.SUPERSEDED)

        result = list_project_improvements(project)

        assert result["improvements"] == []

    def test_includes_custom_monitor_as_custom_analysis(self, project):
        run = _create_run(project)
        custom_item = ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id=f"custom:{uuid4()}",
            item_type=ImprovementItemType.CUSTOM,
            title="Custom monitor issue",
            diagnosis="Custom diagnosis",
            affected_conversations_count=1,
            status=ImprovementItemStatus.ACTIVE,
        )

        result = list_project_improvements(project)

        assert result["improvements"] == [
            {
                "uuid": str(custom_item.uuid),
                "text": "Custom monitor issue",
                "type": "custom_analysis",
                "conversations_count": 1,
            }
        ]

    def test_does_not_append_amazing_conversation_entry(self, project):
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

        result = list_project_improvements(project)

        assert len(result["improvements"]) == 1
        assert result["improvements"][0]["uuid"] == str(item.uuid)


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
        assert response.data == _empty_list_payload()

    def test_returns_active_backlog_item(self, api_client, auth_headers, project):
        run = _create_run(project)
        item = _create_backlog_item(
            run,
            dimension_id="wrong_behavior_due_to_instructions",
            title="Skipped instruction",
            affected_count=3,
        )

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements"] == [
            {
                "uuid": str(item.uuid),
                "text": "Skipped instruction",
                "type": "wrong_behavior_due_to_instructions",
                "conversations_count": 3,
            }
        ]
        assert response.data["improvements_task"]["is_running"] is False

    def test_excludes_superseded_backlog_via_api(self, api_client, auth_headers, project):
        run = _create_run(project)
        _create_backlog_item(run, status=ImprovementItemStatus.SUPERSEDED)

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements"] == []

    def test_returns_custom_analysis_via_api(self, api_client, auth_headers, project):
        run = _create_run(project)
        custom_item = ImprovementBacklogItem.objects.create(
            project=project,
            run=run,
            dimension_id=f"custom:{uuid4()}",
            item_type=ImprovementItemType.CUSTOM,
            title="Custom monitor issue",
            diagnosis="Custom diagnosis",
            affected_conversations_count=1,
            status=ImprovementItemStatus.ACTIVE,
        )

        response = api_client.get(self._url(project.uuid), **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["improvements"][0]["type"] == "custom_analysis"
        assert response.data["improvements"][0]["uuid"] == str(custom_item.uuid)
