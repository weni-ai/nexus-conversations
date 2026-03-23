"""Tests for sync_project_timezones_task."""

from unittest.mock import Mock, patch

import pytest

from conversation_ms.tasks import (
    _SYNC_PROJECT_TIMEZONES_LOCK_KEY,
    sync_project_timezones_task,
)
from conversation_ms.tests.factories import ProjectFactory


@pytest.fixture(autouse=True)
def _in_memory_tasks_cache_lock():
    """Tasks use Redis in prod; tests patch ``conversation_ms.tasks.cache`` for the sync lock."""
    store: dict = {}

    def add(key, val, timeout=None):
        if key in store:
            return False
        store[key] = val
        return True

    def get(key, default=None):
        return store.get(key, default)

    def delete(key):
        store.pop(key, None)

    with patch("conversation_ms.tasks.cache") as m:
        m.add.side_effect = add
        m.get.side_effect = get
        m.delete.side_effect = delete
        store.clear()
        yield store


@pytest.fixture(autouse=True)
def _patch_close_daily_delay():
    with patch("conversation_ms.tasks.close_daily_conversations_task.delay"):
        yield


@pytest.fixture
def mock_project_client():
    client = Mock()
    client.page_size = 100
    client.get_projects_paginated = Mock()
    return client


@pytest.mark.django_db
class TestSyncProjectTimezonesTask:
    def test_updates_timezone_for_existing_project(self, mock_project_client):
        project = ProjectFactory(timezone=None)
        mock_project_client.get_projects_paginated.return_value = {
            "results": [{"uuid": str(project.uuid), "timezone": "Europe/London"}],
            "next": None,
        }

        result = sync_project_timezones_task(project_client=mock_project_client)

        assert result["status"] == "success"
        assert result["project_rows_updated"] == 1
        project.refresh_from_db()
        assert project.timezone == "Europe/London"

    def test_clears_timezone_when_api_returns_empty(self, mock_project_client):
        project = ProjectFactory(timezone="America/Sao_Paulo")
        mock_project_client.get_projects_paginated.return_value = {
            "results": [{"uuid": str(project.uuid), "timezone": None}],
            "next": None,
        }

        result = sync_project_timezones_task(project_client=mock_project_client)

        assert result["status"] == "success"
        project.refresh_from_db()
        assert project.timezone is None

    def test_skips_unknown_project_uuid(self, mock_project_client):
        mock_project_client.get_projects_paginated.return_value = {
            "results": [{"uuid": "00000000-0000-0000-0000-000000000099", "timezone": "UTC"}],
            "next": None,
        }

        result = sync_project_timezones_task(project_client=mock_project_client)

        assert result["status"] == "success"
        assert result["project_rows_updated"] == 0

    def test_skips_invalid_timezone_from_api(self, mock_project_client):
        project = ProjectFactory(timezone="America/Sao_Paulo")
        mock_project_client.get_projects_paginated.return_value = {
            "results": [{"uuid": str(project.uuid), "timezone": "Not/Azone"}],
            "next": None,
        }

        result = sync_project_timezones_task(project_client=mock_project_client)

        assert result["invalid_timezone_skipped"] == 1
        project.refresh_from_db()
        assert project.timezone == "America/Sao_Paulo"

    def test_paginates_api(self, mock_project_client):
        p1 = ProjectFactory()
        p2 = ProjectFactory()
        mock_project_client.get_projects_paginated.side_effect = [
            {
                "results": [{"uuid": str(p1.uuid), "timezone": "UTC"}],
                "next": "http://example.com/p2",
            },
            {
                "results": [{"uuid": str(p2.uuid), "timezone": "America/New_York"}],
                "next": None,
            },
        ]

        result = sync_project_timezones_task(project_client=mock_project_client)

        assert result["status"] == "success"
        assert mock_project_client.get_projects_paginated.call_count == 2
        assert result["project_rows_updated"] == 2
        p1.refresh_from_db()
        p2.refresh_from_db()
        assert p1.timezone == "UTC"
        assert p2.timezone == "America/New_York"

    def test_success_enqueues_close_daily(self, mock_project_client, _in_memory_tasks_cache_lock):
        """After sync, close_daily is enqueued with skip_sync_lock_check while the lock is still held."""
        project = ProjectFactory(timezone=None)
        mock_project_client.get_projects_paginated.return_value = {
            "results": [{"uuid": str(project.uuid), "timezone": "UTC"}],
            "next": None,
        }

        def delay_side_effect(*_a, **kw):
            assert kw.get("skip_sync_lock_check") is True
            assert _SYNC_PROJECT_TIMEZONES_LOCK_KEY in _in_memory_tasks_cache_lock

        with patch(
            "conversation_ms.tasks.close_daily_conversations_task.delay",
            side_effect=delay_side_effect,
        ) as mock_delay:
            sync_project_timezones_task(project_client=mock_project_client)
            mock_delay.assert_called_once_with(skip_sync_lock_check=True)

        assert _SYNC_PROJECT_TIMEZONES_LOCK_KEY not in _in_memory_tasks_cache_lock

    def test_skips_malformed_uuid_row_continues_sync(self, mock_project_client):
        project = ProjectFactory(timezone=None)
        mock_project_client.get_projects_paginated.return_value = {
            "results": [
                {"uuid": "not-a-valid-uuid", "timezone": "UTC"},
                {"uuid": str(project.uuid), "timezone": "Africa/Nairobi"},
            ],
            "next": None,
        }
        result = sync_project_timezones_task(project_client=mock_project_client)
        assert result["status"] == "success"
        assert result["invalid_timezone_skipped"] == 1
        project.refresh_from_db()
        assert project.timezone == "Africa/Nairobi"

    def test_skips_when_lock_already_held(self, mock_project_client, _in_memory_tasks_cache_lock):
        _in_memory_tasks_cache_lock[_SYNC_PROJECT_TIMEZONES_LOCK_KEY] = 1
        result = sync_project_timezones_task(project_client=mock_project_client)
        assert result["status"] == "skipped"
        assert result["reason"] == "sync_already_running"
        mock_project_client.get_projects_paginated.assert_not_called()
