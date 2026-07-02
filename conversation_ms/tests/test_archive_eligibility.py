from datetime import datetime
from datetime import timezone as dt_timezone

import pendulum
import pytest
from freezegun import freeze_time

from conversation_ms.archive.eligibility import (
    apply_retention_filter,
    is_conversation_within_retention,
    retention_cutoff_utc,
)
from conversation_ms.models import Conversation, Project


@pytest.mark.django_db
class TestRetentionCutoffUtc:
    @freeze_time("2026-07-02T15:00:00Z")
    def test_cutoff_uses_project_timezone_start_of_day(self):
        cutoff = retention_cutoff_utc("America/Sao_Paulo", retention_days_override=90)
        expected = (
            pendulum.parse("2026-07-02T15:00:00Z")
            .in_timezone("America/Sao_Paulo")
            .start_of("day")
            .subtract(days=90)
            .in_timezone("UTC")
        )
        assert cutoff == expected

    @freeze_time("2026-07-02T03:30:00Z")
    def test_cutoff_falls_back_when_project_timezone_invalid(self, settings):
        settings.FALLBACK_TIMEZONE = "UTC"
        cutoff = retention_cutoff_utc("Invalid/Timezone", retention_days_override=90)
        assert cutoff == pendulum.parse("2026-04-03T00:00:00Z")


@pytest.mark.django_db
class TestApplyRetentionFilter:
    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Retention Project", timezone="UTC")

    @freeze_time("2026-07-02T12:00:00Z")
    def test_excludes_closed_conversation_past_retention(self, project):
        Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 2, 12, 0, tzinfo=dt_timezone.utc),
        )
        visible = Conversation.objects.create(
            project=project,
            resolution="0",
            end_date=datetime(2026, 4, 4, 12, 0, tzinfo=dt_timezone.utc),
        )

        qs = apply_retention_filter(Conversation.objects.filter(project=project), project.timezone)
        assert list(qs.values_list("uuid", flat=True)) == [visible.uuid]

    @freeze_time("2026-07-02T12:00:00Z")
    def test_in_progress_conversation_always_visible(self, project):
        stale = Conversation.objects.create(
            project=project,
            resolution="2",
            start_date=datetime(2025, 1, 1, 12, 0, tzinfo=dt_timezone.utc),
        )

        qs = apply_retention_filter(Conversation.objects.filter(project=project), project.timezone)
        assert list(qs.values_list("uuid", flat=True)) == [stale.uuid]

    @freeze_time("2026-07-02T12:00:00Z")
    def test_is_conversation_within_retention_matches_queryset(self, project):
        expired = Conversation.objects.create(
            project=project,
            resolution="1",
            end_date=datetime(2026, 4, 2, 12, 0, tzinfo=dt_timezone.utc),
        )
        assert is_conversation_within_retention(expired, project.timezone) is False

        recent = Conversation.objects.create(
            project=project,
            resolution="1",
            end_date=datetime(2026, 4, 4, 12, 0, tzinfo=dt_timezone.utc),
        )
        assert is_conversation_within_retention(recent, project.timezone) is True
