"""
Tests for ProjectDay date helper.
"""

from datetime import date
from uuid import uuid4

import pendulum
import pytest

from conversation_ms.models import Conversation
from conversation_ms.utils.date_helpers import (
    ProjectDay,
    calendar_date_in_project_timezone,
    conversation_effective_service_end_utc,
    end_of_project_local_calendar_day_utc,
    resolve_effective_project_timezone,
)


class TestProjectDay:
    """Tests for ProjectDay class."""

    def test_project_day_for_yesterday(self):
        """Test creating ProjectDay for yesterday."""
        project_day = ProjectDay.for_yesterday("America/Sao_Paulo")

        assert isinstance(project_day, ProjectDay)
        assert project_day.project_timezone == "America/Sao_Paulo"

        # Verify it's actually yesterday
        local_now = pendulum.now("America/Sao_Paulo")
        expected_yesterday = local_now.subtract(days=1).date()
        assert project_day.target_date == expected_yesterday

    def test_project_day_for_date(self):
        """Test creating ProjectDay for specific date."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")

        assert isinstance(project_day, ProjectDay)
        assert project_day.target_date == date(2024, 1, 15)
        assert project_day.project_timezone == "America/Sao_Paulo"

    def test_project_day_for_date_range(self):
        """Test creating ProjectDay for date range."""
        start_day, end_day = ProjectDay.for_date_range("2024-01-15", "2024-01-17", "America/Sao_Paulo")

        assert isinstance(start_day, ProjectDay)
        assert isinstance(end_day, ProjectDay)
        assert start_day.target_date == date(2024, 1, 15)
        assert end_day.target_date == date(2024, 1, 17)

    def test_project_day_for_date_range_single_date(self):
        """Test creating ProjectDay for date range with None end_date."""
        start_day, end_day = ProjectDay.for_date_range("2024-01-15", None, "America/Sao_Paulo")

        assert start_day.target_date == end_day.target_date
        assert start_day.target_date == date(2024, 1, 15)

    def test_project_day_get_utc_range(self):
        """Test getting UTC range from ProjectDay."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")
        start_utc, end_utc = project_day.get_utc_range()

        assert isinstance(start_utc, pendulum.DateTime)
        assert isinstance(end_utc, pendulum.DateTime)
        assert start_utc.timezone_name == "UTC"
        assert end_utc.timezone_name == "UTC"
        assert start_utc < end_utc

    def test_project_day_get_end_date_utc(self):
        """Test getting end date in UTC."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")
        end_date_utc = project_day.get_end_date_utc()

        assert isinstance(end_date_utc, pendulum.DateTime)
        assert end_date_utc.timezone_name == "UTC"
        # Verify it's the same as end_of_day_utc
        assert end_date_utc == project_day.end_of_day_utc
        # When converted back to project timezone, should be end of day (23:59:59.999999)
        end_in_project_tz = end_date_utc.in_timezone("America/Sao_Paulo")
        assert end_in_project_tz.hour == 23
        assert end_in_project_tz.minute == 59

    def test_project_day_get_date_string(self):
        """Test getting date as string."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")
        date_string = project_day.get_date_string()

        assert date_string == "2024-01-15"

    def test_project_day_different_timezones(self):
        """Test ProjectDay with different timezones."""
        # Test America/Sao_Paulo (UTC-3)
        sp_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")
        sp_start, sp_end = sp_day.get_utc_range()

        # Test UTC
        utc_day = ProjectDay.for_date("2024-01-15", "UTC")
        utc_start, utc_end = utc_day.get_utc_range()

        # Test America/New_York (UTC-5 in winter)
        ny_day = ProjectDay.for_date("2024-01-15", "America/New_York")
        ny_start, ny_end = ny_day.get_utc_range()

        # All should represent the same calendar day in their respective timezones
        assert sp_day.target_date == date(2024, 1, 15)
        assert utc_day.target_date == date(2024, 1, 15)
        assert ny_day.target_date == date(2024, 1, 15)

        # But UTC ranges will be different
        assert sp_start != utc_start or sp_end != utc_end
        assert ny_start != utc_start or ny_end != utc_end

        # Verify UTC ranges are correct
        # America/Sao_Paulo is UTC-3, so 2024-01-15 00:00:00 BRT = 2024-01-15 03:00:00 UTC
        assert sp_start.hour == 3  # 00:00 BRT = 03:00 UTC
        assert utc_start.hour == 0  # 00:00 UTC = 00:00 UTC
        # America/New_York is UTC-5 in January, so 00:00 EST = 05:00 UTC
        assert ny_start.hour == 5  # 00:00 EST = 05:00 UTC

    def test_project_day_dst_transitions(self):
        """Test ProjectDay handles DST transitions correctly."""
        # Test a date during DST in America/New_York which observes DST
        spring_day = ProjectDay.for_date("2024-03-10", "America/New_York")  # DST starts
        fall_day = ProjectDay.for_date("2024-11-03", "America/New_York")  # DST ends

        spring_start, spring_end = spring_day.get_utc_range()
        fall_start, fall_end = fall_day.get_utc_range()

        # Both should still represent full days
        assert spring_start < spring_end
        assert fall_start < fall_end

        # The difference should be approximately 24 hours
        spring_duration = spring_end - spring_start
        fall_duration = fall_end - fall_start

        # Should be close to 24 hours (accounting for DST)
        # Spring: 23 hours (spring forward), Fall: 25 hours (fall back)
        assert spring_duration.total_seconds() > 22 * 3600
        assert spring_duration.total_seconds() < 24 * 3600
        assert fall_duration.total_seconds() > 24 * 3600
        assert fall_duration.total_seconds() < 26 * 3600

        # Verify start times are correct
        # Spring: DST starts at 2:00 AM, so 00:00 is still EST (UTC-5) = 05:00 UTC
        assert spring_start.hour == 5
        # Fall: DST ends at 2:00 AM, so 00:00 is still EDT (UTC-4) = 04:00 UTC
        assert fall_start.hour == 4

        # Verify that the day transitions correctly
        # Spring: day starts in EST, ends in EDT (23 hours total due to spring forward)
        spring_start_in_tz = spring_start.in_timezone("America/New_York")
        spring_end_in_tz = spring_end.in_timezone("America/New_York")
        assert spring_start_in_tz.hour == 0  # Starts at midnight EST
        assert spring_end_in_tz.hour == 23  # Ends at 23:59 EDT

        # Fall: day starts in EDT, ends in EST (25 hours total due to fall back)
        fall_start_in_tz = fall_start.in_timezone("America/New_York")
        fall_end_in_tz = fall_end.in_timezone("America/New_York")
        assert fall_start_in_tz.hour == 0  # Starts at midnight EDT
        assert fall_end_in_tz.hour == 23  # Ends at 23:59 EST

    def test_project_day_start_of_day(self):
        """Test that start of day is correctly set."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")

        # Start of day in project timezone should be 00:00:00
        assert project_day.start_of_day_project_tz.hour == 0
        assert project_day.start_of_day_project_tz.minute == 0
        assert project_day.start_of_day_project_tz.second == 0
        assert project_day.start_of_day_project_tz.timezone_name == "America/Sao_Paulo"

    def test_project_day_end_of_day(self):
        """Test that end of day is correctly set."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")

        # End of day in project timezone should be 23:59:59.999999
        assert project_day.end_of_day_project_tz.hour == 23
        assert project_day.end_of_day_project_tz.minute == 59
        assert project_day.end_of_day_project_tz.timezone_name == "America/Sao_Paulo"

    def test_project_day_utc_conversion(self):
        """Test UTC conversion is correct."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")

        # Convert back to project timezone and verify
        start_utc, end_utc = project_day.get_utc_range()
        start_back = start_utc.in_timezone("America/Sao_Paulo")
        end_back = end_utc.in_timezone("America/Sao_Paulo")

        # Should match the original start/end in project timezone
        assert start_back.start_of("day") == project_day.start_of_day_project_tz
        assert end_back.end_of("day") == project_day.end_of_day_project_tz

    def test_project_day_repr(self):
        """Test ProjectDay string representation."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")
        repr_str = repr(project_day)

        assert "ProjectDay" in repr_str
        assert "2024-01-15" in repr_str
        assert "America/Sao_Paulo" in repr_str

    def test_project_day_timezone_with_offset_negative(self):
        """Test ProjectDay with timezone with negative offset (America/Los_Angeles)."""
        la_day = ProjectDay.for_date("2024-01-15", "America/Los_Angeles")
        la_start, la_end = la_day.get_utc_range()

        # Los Angeles is UTC-8 in winter, so 00:00 PST = 08:00 UTC
        assert la_start.hour == 8
        assert la_day.target_date == date(2024, 1, 15)
        assert la_start < la_end

    def test_project_day_immutability(self):
        """Test that ProjectDay attributes are set correctly and consistently."""
        project_day = ProjectDay.for_date("2024-01-15", "America/Sao_Paulo")

        # All attributes should be set
        assert project_day.target_date is not None
        assert project_day.project_timezone is not None
        assert project_day.start_of_day_project_tz is not None
        assert project_day.end_of_day_project_tz is not None
        assert project_day.start_of_day_utc is not None
        assert project_day.end_of_day_utc is not None

        # Verify relationships
        assert project_day.start_of_day_utc == project_day.start_of_day_project_tz.in_timezone("UTC")
        assert project_day.end_of_day_utc == project_day.end_of_day_project_tz.in_timezone("UTC")

    def test_project_day_for_date_range_same_date(self):
        """Test for_date_range when start and end are the same."""
        start_day, end_day = ProjectDay.for_date_range("2024-01-15", "2024-01-15", "America/Sao_Paulo")

        assert start_day.target_date == end_day.target_date
        assert start_day.project_timezone == end_day.project_timezone
        # They should be different instances but represent the same day
        assert start_day.target_date == date(2024, 1, 15)
        assert end_day.target_date == date(2024, 1, 15)

    def test_project_day_utc_timezone_consistency(self):
        """Test that UTC conversions are consistent."""
        project_day = ProjectDay.for_date("2024-01-15", "UTC")

        # When timezone is UTC, start and end should match
        assert project_day.start_of_day_project_tz.timezone_name == "UTC"
        assert project_day.end_of_day_project_tz.timezone_name == "UTC"
        assert project_day.start_of_day_utc == project_day.start_of_day_project_tz
        assert project_day.end_of_day_utc == project_day.end_of_day_project_tz


@pytest.mark.django_db
class TestProjectTimezoneHelpers:
    def test_calendar_date_in_project_timezone(self):
        assert calendar_date_in_project_timezone("2026-06-02T02:00:00Z", "America/Sao_Paulo") == date(2026, 6, 1)

    def test_resolve_effective_stored_valid(self):
        assert resolve_effective_project_timezone("UTC") == "UTC"

    def test_resolve_effective_invalid_uses_fallback(self, settings):
        settings.FALLBACK_TIMEZONE = "UTC"
        assert resolve_effective_project_timezone("Invalid/X") == "UTC"
        assert resolve_effective_project_timezone(None) == "UTC"


class TestEndOfDaySharedWithCloseDaily:
    """end_of_project_local_calendar_day_utc must match ProjectDay / close_daily."""

    def test_matches_project_day_get_end_date_utc(self):
        tz = "America/Sao_Paulo"
        moment = "2026-02-20T12:00:00Z"
        d = calendar_date_in_project_timezone(moment, tz)
        from_helper = end_of_project_local_calendar_day_utc(moment, tz)
        from_project_day = ProjectDay(d, tz).get_end_date_utc()
        assert from_helper == from_project_day


@pytest.mark.django_db
class TestConversationEffectiveServiceEnd:
    def test_none_end_date_uses_canonical_only(self, project):
        start = pendulum.parse("2026-02-20T10:00:00Z")
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="X",
            channel_uuid=uuid4(),
            resolution="2",
            start_date=start,
            end_date=None,
        )
        tz = "America/Sao_Paulo"
        expected = ProjectDay(calendar_date_in_project_timezone(start, tz), tz).get_end_date_utc()
        assert conversation_effective_service_end_utc(conv, tz) == expected

    def test_legacy_long_end_date_caps_at_canonical(self, project):
        """Stored end_date = start + 1 day (old nexus style): effective end is end of local day."""
        start = pendulum.parse("2026-02-20T10:00:00Z")
        stored_long = start.add(days=1)
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="X",
            channel_uuid=uuid4(),
            resolution="2",
            start_date=start,
            end_date=stored_long,
        )
        tz = "America/Sao_Paulo"
        canonical = ProjectDay(calendar_date_in_project_timezone(start, tz), tz).get_end_date_utc()
        assert conversation_effective_service_end_utc(conv, tz) == canonical
        assert canonical < pendulum.instance(stored_long).in_timezone("UTC")
