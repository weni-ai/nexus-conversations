from datetime import date, timedelta
from uuid import uuid4

import pytest
from django.conf import settings
from django.urls import reverse
from freezegun import freeze_time
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Project
from improvements.enums import (
    ImprovementItemStatus,
    ImprovementItemType,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisRun,
    ImprovementBacklogItem,
    ImprovementCustomMonitor,
)
from improvements.utils.time import utc_datetime


def _create_run(
    project: Project,
    *,
    triggered_on_date: date,
    status: str = ImprovementRunStatus.COMPLETED,
    started_at=None,
    completed_at=None,
    target_date: date | None = None,
) -> ImprovementAnalysisRun:
    started = started_at or utc_datetime(triggered_on_date.year, triggered_on_date.month, triggered_on_date.day, 10)
    completed = completed_at
    if status == ImprovementRunStatus.COMPLETED and completed is None:
        completed = started + timedelta(hours=1)
    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date=target_date or triggered_on_date,
        triggered_on_date=triggered_on_date,
        status=status,
        sample_size=2,
        conversations_total=2,
        conversations_processed=2 if status == ImprovementRunStatus.COMPLETED else 0,
        range_start_utc=utc_datetime(triggered_on_date.year, triggered_on_date.month, triggered_on_date.day),
        range_end_utc=utc_datetime(
            triggered_on_date.year,
            triggered_on_date.month,
            triggered_on_date.day,
            23,
            59,
            59,
        ),
        started_at=started,
        completed_at=completed,
    )


def _create_backlog_item(
    run: ImprovementAnalysisRun,
    *,
    dimension_id: str = "missing_static_knowledge",
    status: str = ImprovementItemStatus.ACTIVE,
    first_seen_at=None,
) -> ImprovementBacklogItem:
    kwargs = {
        "project": run.project,
        "run": run,
        "dimension_id": dimension_id,
        "item_type": ImprovementItemType.KNOWLEDGE,
        "title": f"Item {dimension_id}",
        "diagnosis": "Diagnosis",
        "affected_conversations_count": 1,
        "status": status,
    }
    if first_seen_at is not None:
        kwargs["first_seen_at"] = first_seen_at
    return ImprovementBacklogItem.objects.create(**kwargs)


@pytest.mark.django_db
class TestImprovementsMetricsApi:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def auth_headers(self):
        token = "test-metrics-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    @pytest.fixture
    def project_a(self):
        return Project.objects.create(name="Metrics Project A", timezone="UTC")

    @pytest.fixture
    def project_b(self):
        return Project.objects.create(name="Metrics Project B", timezone="UTC")

    def _metrics_url(self):
        return reverse("improvements-metrics")

    def _suggestions_url(self):
        return reverse("improvements-metrics-suggestions-per-project")

    def test_metrics_requires_auth(self, api_client):
        response = api_client.get(self._metrics_url())
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_suggestions_per_project_requires_auth(self, api_client):
        response = api_client.get(self._suggestions_url())
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_partial_date_range_returns_400(self, api_client, auth_headers):
        response = api_client.get(
            self._metrics_url(),
            {"start_date": "2026-05-01"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "start_date" in response.data
        assert "end_date" in response.data

    def test_start_after_end_returns_400(self, api_client, auth_headers):
        response = api_client.get(
            self._metrics_url(),
            {"start_date": "2026-05-10", "end_date": "2026-05-01"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    @freeze_time("2026-06-01T12:00:00Z")
    def test_metrics_snapshot_sections(self, api_client, auth_headers, project_a, project_b):
        run_a1 = _create_run(
            project_a,
            triggered_on_date=date(2026, 5, 1),
            started_at=utc_datetime(2026, 5, 1, 10),
            completed_at=utc_datetime(2026, 5, 1, 11),
        )
        run_a2 = _create_run(
            project_a,
            triggered_on_date=date(2026, 5, 2),
            started_at=utc_datetime(2026, 5, 2, 10),
            completed_at=utc_datetime(2026, 5, 2, 12),
        )
        run_b = _create_run(
            project_b,
            triggered_on_date=date(2026, 5, 3),
            started_at=utc_datetime(2026, 5, 3, 10),
            completed_at=utc_datetime(2026, 5, 3, 10, 30),
        )
        _create_backlog_item(
            run_a1,
            dimension_id="missing_static_knowledge",
            status=ImprovementItemStatus.RESOLVED,
            first_seen_at=utc_datetime(2026, 5, 1, 10, 15),
        )
        _create_backlog_item(
            run_a1,
            dimension_id="missing_static_knowledge",
            status=ImprovementItemStatus.ACTIVE,
            first_seen_at=utc_datetime(2026, 5, 1, 10, 20),
        )
        _create_backlog_item(
            run_a2,
            dimension_id="personality_deviation",
            status=ImprovementItemStatus.IGNORED,
            first_seen_at=utc_datetime(2026, 5, 2, 10, 30),
        )
        _create_backlog_item(
            run_b,
            dimension_id="missing_static_knowledge",
            status=ImprovementItemStatus.SUPERSEDED,
            first_seen_at=utc_datetime(2026, 5, 3, 10, 5),
        )

        orphan = _create_run(
            project_b,
            triggered_on_date=date(2026, 5, 20),
            status=ImprovementRunStatus.POLLING,
            started_at=utc_datetime(2026, 5, 20, 8),
            completed_at=None,
        )

        monitor = ImprovementCustomMonitor.objects.create(
            project=project_a,
            title="Long replies",
            slug="long-replies",
            definition="Agent replies are too long",
        )
        ImprovementCustomMonitor.objects.filter(pk=monitor.pk).update(
            created_at=utc_datetime(2026, 5, 10, 12),
        )

        response = api_client.get(self._metrics_url(), **auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.data

        assert data["start_date"] is None
        assert data["end_date"] is None
        assert data["usage"] == {
            "projects_with_runs": 2,
            "projects_with_more_than_one_run": 2,
            "total_runs": 4,
            "completed_runs": 3,
        }
        assert data["delivery"]["avg_suggestions_per_completed_run"] == pytest.approx(4 / 3, rel=1e-3)
        assert data["delivery"]["projects_with_suggestions_count"] == 2
        assert data["delivery"]["top_improvement_types"][0] == {
            "dimension_id": "missing_static_knowledge",
            "count": 3,
        }
        assert data["actions"] == {
            "resolved_count": 1,
            "ignored_count": 1,
            "active_count": 1,
            "superseded_count": 1,
        }
        assert data["custom_analysis"]["projects_with_active_custom_monitors"] == 1
        assert data["custom_analysis"]["active_custom_monitors_total"] == 1
        assert data["custom_analysis"]["project_uuids"] == [str(project_a.uuid)]
        assert data["runtime"]["avg_duration_seconds"] is not None
        assert data["runtime"]["p50_duration_seconds"] is not None
        assert data["runtime"]["p95_duration_seconds"] is not None
        assert data["runtime"]["avg_seconds_to_first_suggestion"] is not None
        orphan_rows = data["runtime"]["orphan_runs_over_24h"]
        assert len(orphan_rows) == 1
        assert orphan_rows[0]["run_uuid"] == str(orphan.uuid)
        assert orphan_rows[0]["status"] == ImprovementRunStatus.POLLING

    @freeze_time("2026-06-01T12:00:00Z")
    def test_date_filter_applies_to_runs_and_custom_but_not_orphans(
        self,
        api_client,
        auth_headers,
        project_a,
        project_b,
    ):
        in_range = _create_run(
            project_a,
            triggered_on_date=date(2026, 5, 10),
            started_at=utc_datetime(2026, 5, 10, 10),
            completed_at=utc_datetime(2026, 5, 10, 11),
        )
        _create_backlog_item(in_range)
        _create_run(
            project_a,
            triggered_on_date=date(2026, 4, 1),
            started_at=utc_datetime(2026, 4, 1, 10),
            completed_at=utc_datetime(2026, 4, 1, 11),
        )
        orphan = _create_run(
            project_b,
            triggered_on_date=date(2026, 4, 15),
            status=ImprovementRunStatus.QUEUED,
            started_at=utc_datetime(2026, 4, 15, 8),
            completed_at=None,
        )

        monitor_in = ImprovementCustomMonitor.objects.create(
            project=project_a,
            title="In range",
            slug="in-range",
            definition="In range monitor",
        )
        ImprovementCustomMonitor.objects.filter(pk=monitor_in.pk).update(
            created_at=utc_datetime(2026, 5, 12, 9),
        )
        monitor_out = ImprovementCustomMonitor.objects.create(
            project=project_b,
            title="Out of range",
            slug="out-of-range",
            definition="Out of range monitor",
        )
        ImprovementCustomMonitor.objects.filter(pk=monitor_out.pk).update(
            created_at=utc_datetime(2026, 4, 1, 9),
        )

        response = api_client.get(
            self._metrics_url(),
            {"start_date": "2026-05-01", "end_date": "2026-05-31"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.data
        assert data["start_date"] == "2026-05-01"
        assert data["end_date"] == "2026-05-31"
        assert data["usage"]["total_runs"] == 1
        assert data["usage"]["completed_runs"] == 1
        assert data["delivery"]["projects_with_suggestions_count"] == 1
        assert data["custom_analysis"]["project_uuids"] == [str(project_a.uuid)]
        assert data["custom_analysis"]["active_custom_monitors_total"] == 1
        assert len(data["runtime"]["orphan_runs_over_24h"]) == 1
        assert data["runtime"]["orphan_runs_over_24h"][0]["run_uuid"] == str(orphan.uuid)

    @freeze_time("2026-06-01T12:00:00Z")
    def test_suggestions_per_project_pagination(self, api_client, auth_headers):
        projects = [Project.objects.create(name=f"Paginated Project {i}", timezone="UTC") for i in range(3)]
        for index, project in enumerate(projects):
            run = _create_run(
                project,
                triggered_on_date=date(2026, 5, 1 + index),
                started_at=utc_datetime(2026, 5, 1 + index, 10),
            )
            for _ in range(3 - index):
                _create_backlog_item(run, dimension_id=f"dim-{uuid4().hex[:8]}")

        response = api_client.get(
            self._suggestions_url(),
            {"page": 1, "page_size": 2},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.data
        assert data["count"] == 3
        assert data["previous"] is None
        assert data["next"] is not None
        assert "page=2" in data["next"]
        assert len(data["results"]) == 2
        assert data["results"][0]["suggestions_count"] == 3
        assert data["results"][0]["project_uuid"] == str(projects[0].uuid)
        assert data["results"][1]["suggestions_count"] == 2

        page_two = api_client.get(
            self._suggestions_url(),
            {"page": 2, "page_size": 2},
            **auth_headers,
        )
        assert page_two.status_code == status.HTTP_200_OK
        assert page_two.data["count"] == 3
        assert page_two.data["next"] is None
        assert page_two.data["previous"] is not None
        assert len(page_two.data["results"]) == 1
        assert page_two.data["results"][0]["suggestions_count"] == 1

    @freeze_time("2026-06-01T12:00:00Z")
    def test_suggestions_per_project_date_filter(self, api_client, auth_headers, project_a, project_b):
        in_range = _create_run(
            project_a,
            triggered_on_date=date(2026, 5, 10),
            started_at=utc_datetime(2026, 5, 10, 10),
        )
        _create_backlog_item(in_range)
        _create_backlog_item(in_range, dimension_id="personality_deviation")

        out_of_range = _create_run(
            project_b,
            triggered_on_date=date(2026, 4, 1),
            started_at=utc_datetime(2026, 4, 1, 10),
        )
        _create_backlog_item(out_of_range)

        response = api_client.get(
            self._suggestions_url(),
            {"start_date": "2026-05-01", "end_date": "2026-05-31", "page_size": 50},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.data
        assert data["count"] == 1
        assert data["results"] == [
            {
                "project_uuid": str(project_a.uuid),
                "suggestions_count": 2,
                "completed_runs": 1,
            }
        ]
        assert "start_date=2026-05-01" in (data["next"] or "") or data["next"] is None
