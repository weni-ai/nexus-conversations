from unittest.mock import MagicMock, patch

import pytest
from django.conf import settings
from django.core.cache import cache
from django.test import override_settings

from improvements.services.improvements_redbeat_service import (
    RunAlreadyTerminal,
    RunMetadataNotFound,
    get_run_metadata,
    improvements_run_key,
    mark_cancel_requested,
    register_batch_check_schedule,
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
    @patch("improvements.services.improvements_redbeat_service.RedBeatSchedulerEntry")
    def test_register_batch_check_schedule(self, mock_entry_cls):
        settings.IMPROVEMENTS_BATCH_CHECK_INTERVAL_SECONDS = 300
        mock_entry = MagicMock()
        mock_entry_cls.return_value = mock_entry
        batches = [{"batch_id": "b1"}]

        run_key = register_batch_check_schedule("uuid", "2026-05-29", batches)

        assert run_key == improvements_run_key("uuid", "2026-05-29")
        mock_entry.save.assert_called_once()
        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["batches"] == batches

    @patch("improvements.services.improvements_redbeat_service.RedBeatSchedulerEntry")
    def test_unregister_batch_check_schedule(self, mock_entry_cls):
        save_run_metadata("uuid", "2026-05-29", [], status="polling")
        mock_entry = MagicMock()
        mock_entry.key = "redbeat:improvements-batch-check:uuid:2026-05-29"
        mock_entry_cls.return_value = mock_entry

        run_key = unregister_batch_check_schedule("uuid", "2026-05-29", status="completed")

        assert run_key == improvements_run_key("uuid", "2026-05-29")
        mock_entry.delete.assert_called_once()
        metadata = get_run_metadata("uuid", "2026-05-29")
        assert metadata["status"] == "completed"


class TestCancelTaskExceptions:
    def test_run_already_terminal_is_defined(self):
        with pytest.raises(RunAlreadyTerminal):
            raise RunAlreadyTerminal("terminal")
