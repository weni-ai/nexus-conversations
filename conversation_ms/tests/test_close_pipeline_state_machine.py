"""ClosePipelineStateMachine transition tests."""

from uuid import uuid4

import pytest

from conversation_ms.close_daily.constants import ClosePipelineStageStatus
from conversation_ms.close_daily.state_machine import (
    ClosePipelineStateMachine,
    InvalidClosePipelineData,
    InvalidClosePipelineTransition,
)
from conversation_ms.models import ClosePipelineRecord, Conversation


def _open_conversation(project):
    return Conversation.objects.create(
        project=project,
        contact_urn="whatsapp:+5511888888888",
        contact_name="State Machine Contact",
        channel_uuid=uuid4(),
        resolution="2",
    )


@pytest.mark.django_db
class TestClaimAndFailClassify:
    def test_claim_classify_creates_pending_record(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        assert record is not None
        assert record.classify_status == ClosePipelineStageStatus.PENDING
        assert record.classify_pending_at is not None
        assert record.topics_status is None

    def test_claim_classify_is_idempotent_none_on_second_call(self, project):
        conversation = _open_conversation(project)
        first = ClosePipelineStateMachine.claim_classify(conversation)
        second = ClosePipelineStateMachine.claim_classify(conversation)
        assert first is not None
        assert second is None
        assert ClosePipelineRecord.objects.filter(conversation=conversation).count() == 1

    def test_claim_rejects_terminal_conversation(self, project):
        conversation = _open_conversation(project)
        conversation.resolution = "0"
        conversation.save(update_fields=["resolution"])
        assert ClosePipelineStateMachine.claim_classify(conversation) is None

    def test_fail_classify(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        record = ClosePipelineStateMachine.fail_classify(record, "lambda timeout")
        assert record.classify_status == ClosePipelineStageStatus.FAILED
        assert record.classify_error == "lambda timeout"
        assert record.classify_pending_at is None


@pytest.mark.django_db
class TestCommitClassifySuccess:
    def test_commit_moves_to_shape_c(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        record = ClosePipelineStateMachine.commit_classify_success(
            record,
            resolution="0",
            billing_status=ClosePipelineStageStatus.SKIPPED,
        )
        conversation.refresh_from_db()
        assert conversation.resolution == "0"
        assert record.classify_status == ClosePipelineStageStatus.DONE
        assert record.classify_at is not None
        assert record.topics_status == ClosePipelineStageStatus.PENDING
        assert record.billing_status == ClosePipelineStageStatus.SKIPPED
        assert record.billing_at is not None
        assert record.datalake_status == ClosePipelineStageStatus.PENDING
        assert record.datalake_classification_at is None

    def test_commit_rejects_non_terminal_resolution(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        with pytest.raises(InvalidClosePipelineData):
            ClosePipelineStateMachine.commit_classify_success(record, resolution="2")

    def test_commit_rejects_non_pending_datalake_init(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        with pytest.raises(InvalidClosePipelineData):
            ClosePipelineStateMachine.commit_classify_success(
                record,
                resolution="0",
                datalake_status=ClosePipelineStageStatus.SKIPPED,
            )


@pytest.mark.django_db
class TestMarkAndReclaim:
    def _shape_c(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        return ClosePipelineStateMachine.commit_classify_success(record, resolution="0")

    def test_mark_done_and_failed(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_done(record, "topics")
        assert record.topics_status == ClosePipelineStageStatus.DONE
        assert record.topics_at is not None

        record = ClosePipelineStateMachine.mark_failed(record, "billing", "queue missing")
        assert record.billing_status == ClosePipelineStageStatus.FAILED
        assert record.billing_error == "queue missing"

    def test_mark_failed_rejects_empty_error(self, project):
        record = self._shape_c(project)
        with pytest.raises(InvalidClosePipelineData):
            ClosePipelineStateMachine.mark_failed(record, "topics", "  ")

    def test_reclaim_failed_to_pending(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "topics", "boom")
        assert record.topics_reclaim_count == 0
        record = ClosePipelineStateMachine.reclaim_failed(record, "topics")
        assert record.topics_status == ClosePipelineStageStatus.PENDING
        assert record.topics_error is None
        assert record.topics_pending_at is not None
        assert record.topics_reclaim_count == 1

    def test_reclaim_failed_without_budget(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "billing", "sqs down")
        record = ClosePipelineStateMachine.reclaim_failed(record, "billing", consume_budget=False)
        assert record.billing_status == ClosePipelineStageStatus.PENDING
        assert record.billing_reclaim_count == 0

    def test_reclaim_skipped_ops_only(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_skipped(record, "billing")
        record = ClosePipelineStateMachine.reclaim_skipped(record, "billing")
        assert record.billing_status == ClosePipelineStageStatus.PENDING

        with pytest.raises(InvalidClosePipelineTransition):
            ClosePipelineStateMachine.reclaim_skipped(record, "classify")

    def test_reclaim_skipped_rejects_non_skipped(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_done(record, "topics")
        with pytest.raises(InvalidClosePipelineTransition):
            ClosePipelineStateMachine.reclaim_skipped(record, "topics")

    def test_mark_dead_and_reclaim_dead(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_failed(record, "topics", "poison")
        record = ClosePipelineStateMachine.reclaim_failed(record, "topics")
        record = ClosePipelineStateMachine.mark_failed(record, "topics", "poison again")
        record = ClosePipelineStateMachine.mark_dead(record, "topics", "reclaim budget exhausted")
        assert record.topics_status == ClosePipelineStageStatus.DEAD
        assert record.topics_error == "reclaim budget exhausted"
        assert record.topics_pending_at is None
        assert record.topics_reclaim_count == 1

        record = ClosePipelineStateMachine.reclaim_dead(record, "topics")
        assert record.topics_status == ClosePipelineStageStatus.PENDING
        assert record.topics_reclaim_count == 0
        assert record.topics_error is None

    def test_mark_dead_rejects_finished(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_done(record, "topics")
        with pytest.raises(InvalidClosePipelineTransition):
            ClosePipelineStateMachine.mark_dead(record, "topics", "nope")

    def test_reclaim_stale_pending_increments(self, project):
        record = self._shape_c(project)
        before = record.topics_pending_at
        record = ClosePipelineStateMachine.reclaim_stale_pending(record, "topics")
        assert record.topics_status == ClosePipelineStageStatus.PENDING
        assert record.topics_pending_at >= before
        assert record.topics_reclaim_count == 1

    def test_abandon_pipeline_to_shape_e(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        record = ClosePipelineStateMachine.fail_classify(record, "poison")
        record = ClosePipelineStateMachine.mark_dead(record, "classify", "budget exhausted")
        abandoned = ClosePipelineStateMachine.abandon_pipeline(record, resolution="3")
        abandoned.refresh_from_db()
        assert abandoned.resolution == "3"
        assert not ClosePipelineRecord.objects.filter(conversation=abandoned).exists()

    def test_abandon_pipeline_rejects_non_terminal_resolution(self, project):
        conversation = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conversation)
        with pytest.raises(InvalidClosePipelineData):
            ClosePipelineStateMachine.abandon_pipeline(record, resolution="2")

    def test_heartbeat_refreshes_pending_at(self, project):
        record = self._shape_c(project)
        before = record.topics_pending_at
        record = ClosePipelineStateMachine.heartbeat_pending(record, "topics")
        assert record.topics_pending_at >= before

    def test_mark_datalake_events_promote_done(self, project):
        record = self._shape_c(project)
        record = ClosePipelineStateMachine.mark_datalake_event_sent(record, event="classification")
        assert record.datalake_status == ClosePipelineStageStatus.PENDING
        assert record.datalake_classification_at is not None

        record = ClosePipelineStateMachine.mark_datalake_event_sent(record, event="topics")
        assert record.datalake_status == ClosePipelineStageStatus.DONE
        assert record.datalake_at is not None
        assert record.datalake_topics_at is not None
