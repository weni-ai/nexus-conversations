import pytest
from django.core.cache import cache
from django.test import override_settings

from improvements.adapters.in_memory import InMemoryBatchCheckScheduler, build_in_memory_improvements_dependencies
from improvements.dependencies import reset_improvements_dependencies, set_improvements_dependencies
from improvements.services.improvements_redbeat_service import (
    RunAlreadyTerminal,
    RunMetadataNotFound,
    get_run_metadata,
    improvements_run_key,
    mark_cancel_requested,
    register_batch_check_schedule,
    run_schedule_exists,
    save_run_metadata,
    unregister_batch_check_schedule,
)


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

    def test_get_run_metadata_raises_when_missing(self):
        with pytest.raises(RunMetadataNotFound):
            get_run_metadata("uuid", "2026-05-29")

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
