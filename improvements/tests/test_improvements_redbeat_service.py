from unittest.mock import patch

import pendulum
import pytest
from django.core.cache import cache
from django.test import override_settings

from improvements.adapters.in_memory import InMemoryBatchCheckScheduler, build_in_memory_improvements_dependencies
from improvements.dependencies import reset_improvements_dependencies, set_improvements_dependencies
from improvements.services.improvements_redbeat_service import (
    RunAlreadyTerminal,
    RunMetadataNotFound,
    _metadata_cache_key,
    _schedule_registered_at_cache_key,
    get_run_metadata,
    improvements_run_key,
    is_polling_past_timeout,
    mark_cancel_requested,
    register_batch_check_schedule,
    run_schedule_exists,
    save_run_metadata,
    unregister_batch_check_schedule,
)
from improvements.utils.time import format_schedule_registered_at, polling_elapsed_seconds


@pytest.fixture(autouse=True)
def use_locmem_cache():
    locmem_settings = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }
    with override_settings(CACHES=locmem_settings):
        cache.clear()
        yield
        cache.clear()


class TestRunMetadata:
    def test_save_and_get_run_metadata(self):
        batches = [{"batch_id": "b1", "input_file_id": "f1"}]
        save_run_metadata("uuid", "2026-05-29", batches)

        metadata = get_run_metadata("uuid", "2026-05-29")

        assert metadata["batches"] == batches
        assert metadata["cancel_requested"] is False
        assert metadata["status"] == "polling"
        assert metadata["schedule_registered_at"]

    def test_save_run_metadata_preserves_schedule_registered_at(self):
        first = save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
        original_registered_at = first["schedule_registered_at"]

        second = save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b2"}])

        assert second["schedule_registered_at"] == original_registered_at

    def test_update_run_metadata_does_not_overwrite_schedule_registered_at(self):
        save_run_metadata("uuid", "2026-05-29", [])
        original_registered_at = get_run_metadata("uuid", "2026-05-29")["schedule_registered_at"]

        from improvements.services.improvements_redbeat_service import update_run_metadata

        update_run_metadata("uuid", "2026-05-29", status="partial", schedule_registered_at="ignored")

        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["status"] == "partial"
        assert metadata["schedule_registered_at"] == original_registered_at

    def test_get_run_metadata_raises_when_missing(self):
        with pytest.raises(RunMetadataNotFound):
            get_run_metadata("uuid", "2026-05-29")

    @patch("improvements.services.improvements_redbeat_service.format_schedule_registered_at")
    def test_concurrent_initial_save_uses_single_schedule_registered_at(self, mock_format):
        mock_format.side_effect = ["2026-06-22T10:00:00Z", "2026-06-22T10:00:01Z"]
        real_add = cache.add
        add_calls = {"count": 0}

        def add_side_effect(key, value, timeout):
            if key.endswith(":schedule_registered_at"):
                add_calls["count"] += 1
                if add_calls["count"] == 1:
                    return real_add(key, value, timeout)
                return False
            return real_add(key, value, timeout)

        with patch.object(cache, "add", side_effect=add_side_effect):
            first = save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])
            second = save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b2"}])

        assert first["schedule_registered_at"] == "2026-06-22T10:00:00Z"
        assert second["schedule_registered_at"] == "2026-06-22T10:00:00Z"
        assert mock_format.call_count == 1

    def test_legacy_metadata_backfills_dedicated_key(self):
        metadata_key = _metadata_cache_key("uuid", "2026-05-29")
        schedule_key = _schedule_registered_at_cache_key(metadata_key)
        legacy_metadata = {
            "batches": [],
            "cancel_requested": False,
            "status": "polling",
            "schedule_registered_at": "2026-01-01T00:00:00Z",
        }
        cache.set(metadata_key, legacy_metadata, 604800)

        result = save_run_metadata("uuid", "2026-05-29", [{"batch_id": "b1"}])

        assert result["schedule_registered_at"] == "2026-01-01T00:00:00Z"
        assert cache.get(schedule_key) == "2026-01-01T00:00:00Z"

    def test_mark_cancel_requested(self):
        save_run_metadata("uuid", "2026-05-29", [])
        mark_cancel_requested("uuid", "2026-05-29")

        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["cancel_requested"] is True
        assert metadata["status"] == "cancelling"


class TestRedBeatSchedule:
    def test_register_batch_check_schedule(self):
        scheduler = InMemoryBatchCheckScheduler()
        set_improvements_dependencies(build_in_memory_improvements_dependencies(scheduler=scheduler))
        batches = [{"batch_id": "b1"}]

        run_key = register_batch_check_schedule("uuid", "2026-05-29", batches)

        assert run_key == improvements_run_key("uuid", "2026-05-29")
        assert run_schedule_exists("uuid", "2026-05-29")
        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["batches"] == batches
        assert metadata["schedule_registered_at"]
        reset_improvements_dependencies()

    def test_unregister_batch_check_schedule(self):
        scheduler = InMemoryBatchCheckScheduler()
        set_improvements_dependencies(build_in_memory_improvements_dependencies(scheduler=scheduler))
        save_run_metadata("uuid", "2026-05-29", [], status="polling")
        register_batch_check_schedule("uuid", "2026-05-29", [])

        run_key = unregister_batch_check_schedule("uuid", "2026-05-29", status="completed")

        assert run_key == improvements_run_key("uuid", "2026-05-29")
        assert not run_schedule_exists("uuid", "2026-05-29")
        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["status"] == "completed"
        reset_improvements_dependencies()


class TestCancelTaskExceptions:
    def test_run_already_terminal_is_defined(self):
        with pytest.raises(RunAlreadyTerminal):
            raise RunAlreadyTerminal("terminal")


class TestPollingTimeout:
    @override_settings(IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS=3600)
    def test_is_polling_past_timeout_when_elapsed(self):
        registered_at = format_schedule_registered_at(pendulum.now("UTC").subtract(hours=2))
        metadata = {
            "status": "partial",
            "schedule_registered_at": registered_at,
            "batches": [],
        }

        assert is_polling_past_timeout(metadata, run=None) is True

    @override_settings(IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS=3600)
    def test_is_polling_past_timeout_when_within_window(self):
        registered_at = format_schedule_registered_at(pendulum.now("UTC").subtract(minutes=30))
        metadata = {
            "status": "partial",
            "schedule_registered_at": registered_at,
            "batches": [],
        }

        assert is_polling_past_timeout(metadata, run=None) is False

    @override_settings(IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS=3600)
    def test_is_polling_past_timeout_false_for_terminal_metadata(self):
        registered_at = format_schedule_registered_at(pendulum.now("UTC").subtract(hours=2))
        metadata = {
            "status": "cancelled",
            "schedule_registered_at": registered_at,
            "batches": [],
        }

        assert is_polling_past_timeout(metadata, run=None) is False

    def test_polling_elapsed_seconds_uses_pendulum(self):
        registered_at = format_schedule_registered_at(pendulum.now("UTC").subtract(hours=1, minutes=5))

        elapsed = polling_elapsed_seconds(registered_at)

        assert 3900 <= elapsed <= 4000
