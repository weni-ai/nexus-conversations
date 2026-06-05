from datetime import date

import pendulum
import pytest
from django.conf import settings
from django.urls import reverse
from freezegun import freeze_time
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Conversation, Project
from conversation_ms.services.resolution_summary import (
    aggregate_resolution_summary,
    default_calendar_range_for_timezone,
    parse_project_uuids,
    resolve_calendar_range,
)
from conversation_ms.utils.date_helpers import ProjectDay


@pytest.mark.django_db
class TestResolutionSummaryService:
    @pytest.fixture
    def project_a(self):
        return Project.objects.create(name="Project A")

    @pytest.fixture
    def project_b(self):
        return Project.objects.create(name="Project B")

    def _dt(self, year, month, day, hour=12, tz="UTC"):
        return pendulum.datetime(year, month, day, hour, 0, 0, tz=tz)

    def test_default_calendar_range_last_seven_days_from_yesterday(self):
        with freeze_time("2026-05-26T12:00:00Z"):
            start, end = default_calendar_range_for_timezone("America/Sao_Paulo")
        assert start == date(2026, 5, 19)
        assert end == date(2026, 5, 25)

    def test_conversation_counts_by_resolution_rules(self, project_a):
        in_window = self._dt(2026, 5, 20)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window)
        Conversation.objects.create(project=project_a, resolution="1", start_date=in_window)
        Conversation.objects.create(project=project_a, resolution="2", start_date=in_window)
        Conversation.objects.create(project=project_a, resolution="3", start_date=in_window)
        Conversation.objects.create(project=project_a, resolution="4", start_date=in_window)

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        row = payload["projects"][0]
        assert row["conversation_count"] == 5
        assert row["resolved_count"] == 1
        assert row["unresolved_count"] == 1
        assert row["human_support_count"] == 1
        assert row["resolution_rate"] == pytest.approx(0.5)

    def test_excludes_conversations_outside_start_date_window(self, project_a):
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 10),
        )
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 20),
        )

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        assert payload["projects"][0]["conversation_count"] == 1

    def test_without_project_uuids_returns_only_projects_with_conversations_in_range(self, project_a, project_b):
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 20),
        )

        payload = aggregate_resolution_summary(
            project_uuids=None,
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        assert len(payload["projects"]) == 1
        assert payload["projects"][0]["project_uuid"] == str(project_a.uuid)

    def test_project_without_conversations_returns_zeros(self, project_a, project_b):
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 20),
        )

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid, project_b.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        by_uuid = {row["project_uuid"]: row for row in payload["projects"]}
        assert by_uuid[str(project_b.uuid)]["conversation_count"] == 0
        assert by_uuid[str(project_b.uuid)]["resolution_rate"] is None

    def test_csat_valid_range_and_invalid_ignored(self, project_a):
        in_window = self._dt(2026, 5, 20)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, csat="5")
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, csat="3")
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, csat="9")

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        row = payload["projects"][0]
        assert row["csat_responses_count"] == 2
        assert row["csat"] == pytest.approx(4.0)
        assert payload["average_csat"] == pytest.approx(4.0)

    def test_project_without_csat_returns_null(self, project_a):
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 20),
        )

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        row = payload["projects"][0]
        assert row["csat"] is None
        assert row["csat_responses_count"] == 0
        assert payload["average_csat"] is None

    def test_nps_valid_range_and_invalid_ignored(self, project_a):
        in_window = self._dt(2026, 5, 20)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, nps=10)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, nps=0)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, nps=11)

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        row = payload["projects"][0]
        assert row["nps_responses_count"] == 2
        assert row["nps"] == pytest.approx(5.0)
        assert payload["average_nps"] == pytest.approx(5.0)

    def test_average_resolution_rate_is_arithmetic_mean_of_project_rates(self, project_a, project_b):
        Conversation.objects.create(
            project=project_a,
            resolution="0",
            start_date=self._dt(2026, 5, 20),
        )
        Conversation.objects.create(
            project=project_b,
            resolution="1",
            start_date=self._dt(2026, 5, 20),
        )

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid, project_b.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        assert payload["average_resolution_rate"] == pytest.approx(0.5)

    def test_project_with_only_non_evaluable_resolutions_has_null_rate(self, project_a, project_b):
        in_window = self._dt(2026, 5, 20)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window)
        Conversation.objects.create(project=project_b, resolution="2", start_date=in_window)
        Conversation.objects.create(project=project_b, resolution="3", start_date=in_window)

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid, project_b.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        by_uuid = {row["project_uuid"]: row for row in payload["projects"]}
        assert by_uuid[str(project_a.uuid)]["resolution_rate"] == pytest.approx(1.0)
        assert by_uuid[str(project_b.uuid)]["resolution_rate"] is None
        assert payload["average_resolution_rate"] == pytest.approx(1.0)

    def test_weighted_average_csat_across_projects(self, project_a, project_b):
        in_window = self._dt(2026, 5, 20)
        Conversation.objects.create(project=project_a, resolution="0", start_date=in_window, csat="5")
        Conversation.objects.create(project=project_b, resolution="0", start_date=in_window, csat="1")

        payload = aggregate_resolution_summary(
            project_uuids=[project_a.uuid, project_b.uuid],
            start_date=date(2026, 5, 19),
            end_date=date(2026, 5, 25),
        )
        assert payload["average_csat"] == pytest.approx(3.0)

    def test_resolve_calendar_range_requires_both_dates_when_one_provided(self):
        with pytest.raises(ValueError, match="both be provided"):
            resolve_calendar_range(date(2026, 5, 1), None)

    def test_resolve_calendar_range_rejects_start_date_before_go_live(self):
        with pytest.raises(ValueError, match="start_date must be on or after 2026-03-28"):
            resolve_calendar_range(date(2026, 3, 27), date(2026, 5, 25))

    def test_resolve_calendar_range_rejects_end_date_before_go_live(self):
        with pytest.raises(ValueError, match="end_date must be on or after 2026-03-28"):
            resolve_calendar_range(date(2026, 4, 1), date(2026, 3, 27))

    def test_filters_by_project_timezone_not_utc_midnight(self):
        project = Project.objects.create(
            name="TZ Project",
            timezone="America/Sao_Paulo",
        )
        start_utc, _end_utc = ProjectDay.for_date("2026-05-20", "America/Sao_Paulo").get_utc_range()
        inside = start_utc
        before = start_utc.subtract(seconds=1)

        Conversation.objects.create(project=project, resolution="0", start_date=before)
        Conversation.objects.create(project=project, resolution="0", start_date=inside)

        payload = aggregate_resolution_summary(
            project_uuids=[project.uuid],
            start_date=date(2026, 5, 20),
            end_date=date(2026, 5, 20),
        )
        row = payload["projects"][0]
        assert row["conversation_count"] == 1
        assert row["resolved_count"] == 1

    def test_parse_project_uuids_rejects_invalid(self):
        with pytest.raises(ValueError, match="Invalid project UUID"):
            parse_project_uuids(["not-a-uuid"])


@pytest.mark.django_db
class TestProjectsResolutionSummaryView:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Summary Project", timezone="America/Sao_Paulo")

    @pytest.fixture
    def auth_headers(self):
        token = "test-secret-token"
        settings.INTERNAL_API_TOKENS = {"TestTeam": token}
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_requires_auth(self, api_client):
        url = reverse("projects-resolution-summary")
        response = api_client.get(url)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_project_uuid_returns_400(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(url, {"project_uuids": "bad-uuid"}, **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "project_uuids" in response.data

    def test_invalid_date_returns_400(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(url, {"start_date": "not-a-date"}, **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_partial_date_range_returns_400(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(url, {"start_date": "2026-05-01"}, **auth_headers)
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_start_after_end_returns_400_on_both_fields(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {"start_date": "2026-05-10", "end_date": "2026-05-01"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "start_date" in response.data
        assert "end_date" in response.data

    def test_start_date_before_go_live_returns_400(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {"start_date": "2026-03-27", "end_date": "2026-05-25"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "start_date" in response.data

    def test_end_date_before_go_live_returns_400(self, api_client, auth_headers):
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {"start_date": "2026-04-01", "end_date": "2026-03-27"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "end_date" in response.data

    @freeze_time("2026-05-26T12:00:00Z")
    def test_success_response_shape(self, api_client, project, auth_headers):
        Conversation.objects.create(
            project=project,
            resolution="0",
            start_date=self._dt(2026, 5, 20, 10),
            csat="4",
            nps=8,
        )
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {
                "project_uuids": str(project.uuid),
                "start_date": "2026-05-19",
                "end_date": "2026-05-25",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["start_date"] == "2026-05-19"
        assert response.data["end_date"] == "2026-05-25"
        assert len(response.data["projects"]) == 1
        project_row = response.data["projects"][0]
        assert project_row["conversation_count"] == 1
        assert project_row["resolved_count"] == 1
        assert project_row["resolution_rate"] == pytest.approx(1.0)
        assert project_row["csat"] == pytest.approx(4.0)
        assert project_row["nps"] == pytest.approx(8.0)
        assert response.data["average_resolution_rate"] == pytest.approx(1.0)
        assert response.data["average_csat"] == pytest.approx(4.0)
        assert response.data["average_nps"] == pytest.approx(8.0)

    def _dt(self, year, month, day, hour=12, tz="UTC"):
        return pendulum.datetime(year, month, day, hour, 0, 0, tz=tz)

    @freeze_time("2026-05-26T12:00:00Z")
    def test_default_date_window_when_dates_omitted(self, api_client, project, auth_headers):
        Conversation.objects.create(
            project=project,
            resolution="0",
            start_date=self._dt(2026, 5, 25, 15),
        )
        url = reverse("projects-resolution-summary")
        response = api_client.get(url, {"project_uuids": str(project.uuid)}, **auth_headers)
        assert response.status_code == status.HTTP_200_OK
        assert response.data["start_date"] == "2026-05-19"
        assert response.data["end_date"] == "2026-05-25"
        assert response.data["projects"][0]["conversation_count"] == 1

    def test_without_project_uuids_returns_projects_with_data(self, api_client, auth_headers, project):
        Conversation.objects.create(
            project=project,
            resolution="0",
            start_date=self._dt(2026, 5, 20, 10),
        )
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {"start_date": "2026-05-19", "end_date": "2026-05-25"},
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["projects"]) == 1
        assert response.data["projects"][0]["project_uuid"] == str(project.uuid)

    def test_response_contract_for_nexus_ai_consumer(self, api_client, project, auth_headers):
        """Fields required by nexus-ai projects_resolution_rate merge."""
        Conversation.objects.create(
            project=project,
            resolution="0",
            start_date=self._dt(2026, 5, 20, 10),
            csat="4",
            nps=8,
        )
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {
                "project_uuids": str(project.uuid),
                "start_date": "2026-05-19",
                "end_date": "2026-05-25",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        for key in (
            "start_date",
            "end_date",
            "average_resolution_rate",
            "average_csat",
            "average_nps",
            "projects",
        ):
            assert key in response.data
        row = response.data["projects"][0]
        for key in (
            "project_uuid",
            "conversation_count",
            "resolved_count",
            "unresolved_count",
            "human_support_count",
            "resolution_rate",
            "csat",
            "csat_responses_count",
            "nps",
            "nps_responses_count",
        ):
            assert key in row

    def test_comma_separated_project_uuids(self, api_client, auth_headers, project):
        other = Project.objects.create(name="Other")
        Conversation.objects.create(
            project=project,
            resolution="0",
            start_date=self._dt(2026, 5, 20, 10),
        )
        url = reverse("projects-resolution-summary")
        response = api_client.get(
            url,
            {
                "project_uuids": f"{project.uuid},{other.uuid}",
                "start_date": "2026-05-19",
                "end_date": "2026-05-25",
            },
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert len(response.data["projects"]) == 2
