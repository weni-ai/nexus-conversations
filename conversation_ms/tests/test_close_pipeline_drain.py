"""Close-pipeline drain tests (NEXUS-5774)."""

from datetime import timedelta
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_DEAD_BUDGET_EXHAUSTED,
    ClosePipelineStageStatus,
)
from conversation_ms.close_daily.drain import run_close_pipeline_drain
from conversation_ms.close_daily.metrics import collect_drain_snapshot, count_datalake_blocked_by_topics_dead
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord, Conversation


def _open_conversation(project, **kwargs):
    defaults = dict(
        project=project,
        contact_urn=f"whatsapp:+5511{uuid4().hex[:8]}",
        contact_name="Drain",
        channel_uuid=uuid4(),
        resolution="0",
        start_date=timezone.now(),
        end_date=timezone.now(),
    )
    defaults.update(kwargs)
    return Conversation.objects.create(**defaults)


def _shape_c(project, *, topics_status=ClosePipelineStageStatus.PENDING, **kwargs):
    conv = _open_conversation(project, resolution="2", **kwargs)
    record = ClosePipelineStateMachine.claim_classify(conv)
    return ClosePipelineStateMachine.commit_classify_success(
        record,
        resolution="0",
        topics_status=topics_status,
        billing_status=ClosePipelineStageStatus.PENDING,
        datalake_status=ClosePipelineStageStatus.PENDING,
    )


@pytest.mark.django_db
class TestDrainStaleAndFailed:
    @override_settings(CLOSE_PIPELINE_STALE_PENDING_SECONDS=60, CLOSE_PIPELINE_DRAIN_BATCH_SIZE=100)
    @patch("conversation_ms.close_daily.drain.enqueue_billing")
    def test_stale_pending_reenqueue(self, mock_enq, project):
        record = _shape_c(project)
        stale = timezone.now() - timedelta(seconds=120)
        ClosePipelineRecord.objects.filter(pk=record.pk).update(billing_pending_at=stale)
        record.refresh_from_db()

        tick = run_close_pipeline_drain()

        record.refresh_from_db()
        assert tick["stages"]["billing"]["requeued"] == 1
        assert record.billing_status == ClosePipelineStageStatus.PENDING
        assert record.billing_reclaim_count == 1
        assert record.billing_pending_at > stale
        mock_enq.assert_called_once_with(str(record.conversation_id))

    @override_settings(CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5)
    @patch("conversation_ms.close_daily.drain.enqueue_billing")
    def test_exhausted_reclaim_marks_dead(self, mock_enq, project):
        record = _shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "billing", "sqs down")
        ClosePipelineRecord.objects.filter(pk=record.pk).update(billing_reclaim_count=5)
        record.refresh_from_db()

        tick = run_close_pipeline_drain()

        record.refresh_from_db()
        assert tick["stages"]["billing"]["marked_dead"] == 1
        assert record.billing_status == ClosePipelineStageStatus.DEAD
        assert CLOSE_PIPELINE_DEAD_BUDGET_EXHAUSTED in (record.billing_error or "")
        mock_enq.assert_not_called()

    @override_settings(CLOSE_PIPELINE_STALE_PENDING_SECONDS=60)
    @patch("conversation_ms.close_daily.drain.enqueue_datalake")
    def test_skips_datalake_waiting_on_topics(self, mock_enq, project):
        record = _shape_c(project, topics_status=ClosePipelineStageStatus.PENDING)
        stale = timezone.now() - timedelta(seconds=120)
        ClosePipelineRecord.objects.filter(pk=record.pk).update(
            datalake_pending_at=stale,
            datalake_classification_at=timezone.now(),
        )

        tick = run_close_pipeline_drain()

        record.refresh_from_db()
        assert tick["stages"]["datalake"]["requeued"] == 0
        assert record.datalake_status == ClosePipelineStageStatus.PENDING
        assert record.datalake_reclaim_count == 0
        mock_enq.assert_not_called()

    @override_settings(CLOSE_PIPELINE_STALE_PENDING_SECONDS=60)
    @patch("conversation_ms.close_daily.drain.enqueue_datalake")
    def test_datalake_stale_with_null_classification_at_requeues(self, mock_enq, project):
        record = _shape_c(project, topics_status=ClosePipelineStageStatus.PENDING)
        stale = timezone.now() - timedelta(seconds=120)
        ClosePipelineRecord.objects.filter(pk=record.pk).update(
            datalake_pending_at=stale,
            datalake_classification_at=None,
        )

        tick = run_close_pipeline_drain()

        record.refresh_from_db()
        assert tick["stages"]["datalake"]["requeued"] == 1
        assert record.datalake_reclaim_count == 1
        mock_enq.assert_called_once()

    @patch("conversation_ms.close_daily.drain.enqueue_classify")
    def test_does_not_invent_shape_e(self, mock_enq, project):
        # Terminal conversation without pipeline record (Shape E / legacy hole).
        _open_conversation(project, resolution="0")
        before = ClosePipelineRecord.objects.count()

        run_close_pipeline_drain()

        assert ClosePipelineRecord.objects.count() == before
        mock_enq.assert_not_called()

    @patch("conversation_ms.close_daily.drain.enqueue_billing")
    def test_never_auto_reclaims_skipped_or_dead(self, mock_enq, project):
        record = _shape_c(project)
        record = ClosePipelineStateMachine.mark_skipped(record, "billing")
        dead_record = _shape_c(project)
        dead_record = ClosePipelineStateMachine.mark_failed(dead_record, "billing", "x")
        dead_record = ClosePipelineStateMachine.mark_dead(dead_record, "billing", "budget")

        run_close_pipeline_drain()

        record.refresh_from_db()
        dead_record.refresh_from_db()
        assert record.billing_status == ClosePipelineStageStatus.SKIPPED
        assert dead_record.billing_status == ClosePipelineStageStatus.DEAD
        mock_enq.assert_not_called()


@pytest.mark.django_db
class TestDrainBillingPause:
    @override_settings(CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE=True, CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS=5)
    @patch("conversation_ms.close_daily.drain.enqueue_billing")
    def test_pause_requeues_without_budget_or_dead(self, mock_enq, project):
        record = _shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "billing", "sqs brownout")
        ClosePipelineRecord.objects.filter(pk=record.pk).update(billing_reclaim_count=5)
        record.refresh_from_db()

        tick = run_close_pipeline_drain()

        record.refresh_from_db()
        assert tick["stages"]["billing"]["requeued"] == 1
        assert tick["stages"]["billing"]["marked_dead"] == 0
        assert record.billing_status == ClosePipelineStageStatus.PENDING
        assert record.billing_reclaim_count == 5
        mock_enq.assert_called_once()


@pytest.mark.django_db
class TestDrainTopicsDeadPartial:
    @override_settings(CLOSE_PIPELINE_STALE_PENDING_SECONDS=60)
    @patch("conversation_ms.close_daily.drain.enqueue_datalake")
    def test_topics_dead_leaves_datalake_pending_partial(self, mock_enq, project):
        record = _shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "topics", "poison")
        record = ClosePipelineStateMachine.mark_dead(record, "topics", "budget exhausted")
        stale = timezone.now() - timedelta(seconds=120)
        ClosePipelineRecord.objects.filter(pk=record.pk).update(
            datalake_pending_at=stale,
            datalake_classification_at=timezone.now(),
            datalake_topics_at=None,
        )

        run_close_pipeline_drain()

        record.refresh_from_db()
        assert record.datalake_status == ClosePipelineStageStatus.PENDING
        assert record.datalake_topics_at is None
        assert count_datalake_blocked_by_topics_dead() >= 1
        snapshot = collect_drain_snapshot()
        assert snapshot["datalake_blocked_by_topics_dead"] >= 1
        mock_enq.assert_not_called()


@pytest.mark.django_db
class TestBulkReopenCommand:
    @patch("conversation_ms.management.commands.reopen_close_pipeline_dead.enqueue_billing")
    def test_bulk_dead_to_pending(self, mock_enq, project):
        record = _shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "billing", "sqs")
        record = ClosePipelineStateMachine.mark_dead(record, "billing", "budget exhausted")
        ClosePipelineRecord.objects.filter(pk=record.pk).update(billing_reclaim_count=5)

        out = StringIO()
        call_command("reopen_close_pipeline_dead", stage="billing", enqueue=True, stdout=out)

        record.refresh_from_db()
        assert record.billing_status == ClosePipelineStageStatus.PENDING
        assert record.billing_reclaim_count == 0
        mock_enq.assert_called_once_with(str(record.conversation_id))
        assert "reopened=1" in out.getvalue()


@pytest.mark.django_db
class TestDrainBatchCap:
    @override_settings(CLOSE_PIPELINE_DRAIN_BATCH_SIZE=2, CLOSE_PIPELINE_STALE_PENDING_SECONDS=60)
    @patch("conversation_ms.close_daily.drain.enqueue_billing")
    def test_respects_batch_size(self, mock_enq, project):
        for _ in range(4):
            record = _shape_c(project)
            ClosePipelineStateMachine.mark_failed(record, "billing", "fail")

        tick = run_close_pipeline_drain()

        assert tick["stages"]["billing"]["requeued"] == 2
        assert mock_enq.call_count == 2
