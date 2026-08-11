"""Close-pipeline cutover stage worker / selector tests."""

from unittest.mock import patch
from uuid import uuid4

import pytest
from django.test import override_settings
from django.utils import timezone

from conversation_ms.close_daily.constants import ClosePipelineStageStatus
from conversation_ms.close_daily.runner import _process_conversation_batch
from conversation_ms.close_daily.stages.billing import BillingConfigError, run_billing_stage
from conversation_ms.close_daily.stages.classify import run_classify_stage
from conversation_ms.close_daily.stages.datalake import run_datalake_stage
from conversation_ms.close_daily.stages.topics import run_topics_stage
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord, Conversation


def _open_conversation(project, **kwargs):
    defaults = dict(
        project=project,
        contact_urn="whatsapp:+5511999000000",
        contact_name="Cutover",
        channel_uuid=uuid4(),
        resolution="2",
        start_date=timezone.now(),
        end_date=timezone.now(),
    )
    defaults.update(kwargs)
    return Conversation.objects.create(**defaults)


@pytest.mark.django_db
class TestSelectorClaimEnqueue:
    @patch("conversation_ms.close_daily.runner.enqueue_classify")
    def test_batch_claims_and_enqueues_classify(self, mock_enqueue, project):
        conv = _open_conversation(project)
        claimed = _process_conversation_batch([conv], str(project.uuid), timezone.now())
        assert claimed == 1
        mock_enqueue.assert_called_once_with(str(conv.uuid))
        record = ClosePipelineRecord.objects.get(conversation=conv)
        assert record.classify_status == ClosePipelineStageStatus.PENDING

    @patch("conversation_ms.close_daily.runner.enqueue_classify")
    def test_batch_skips_already_claimed(self, mock_enqueue, project):
        conv = _open_conversation(project)
        ClosePipelineStateMachine.claim_classify(conv)
        claimed = _process_conversation_batch([conv], str(project.uuid), timezone.now())
        assert claimed == 0
        mock_enqueue.assert_not_called()


@pytest.mark.django_db
class TestClassifyStage:
    @patch("conversation_ms.close_daily.stages.classify.enqueue_downstream_after_classify")
    @patch("conversation_ms.close_daily.stages.classify.ClassificationService")
    @patch("conversation_ms.close_daily.stages.classify.MessageMigrationService")
    def test_commits_shape_c_and_enqueues_downstream(self, mock_migration_cls, mock_service_cls, mock_enqueue, project):
        conv = _open_conversation(project)
        record = ClosePipelineStateMachine.claim_classify(conv)

        mock_migration_cls.return_value.persist_conversation_messages_to_postgres.return_value = {
            "persisted": True,
            "messages": [{"text": "hi", "source": "user"}],
        }
        service = mock_service_cls.return_value
        service.classify_resolution.return_value = (conv, "0", [{"text": "hi"}])
        service._get_topics_payload.return_value = [{"topic_uuid": "t"}]

        run_classify_stage(str(conv.uuid))

        record.refresh_from_db()
        conv.refresh_from_db()
        assert conv.resolution == "0"
        assert record.classify_status == ClosePipelineStageStatus.DONE
        assert record.topics_status == ClosePipelineStageStatus.PENDING
        assert record.billing_status == ClosePipelineStageStatus.PENDING
        assert record.datalake_status == ClosePipelineStageStatus.PENDING
        mock_enqueue.assert_called_once_with(str(conv.uuid))


@pytest.mark.django_db
class TestTopicsAndDatalake:
    @patch("conversation_ms.close_daily.stages.datalake.send_data_lake_event")
    @patch("conversation_ms.close_daily.stages.topics.enqueue_datalake")
    @patch("conversation_ms.close_daily.stages.topics.ClassificationService")
    def test_topics_skipped_enqueues_datalake_and_publishes_both(
        self, mock_service_cls, mock_enqueue_dl, mock_send, project
    ):
        conv = _open_conversation(project, resolution="0")
        now = timezone.now()
        record = ClosePipelineRecord.objects.create(
            conversation=conv,
            classify_status=ClosePipelineStageStatus.DONE,
            classify_at=now,
            topics_status=ClosePipelineStageStatus.SKIPPED,
            topics_at=now,
            billing_status=ClosePipelineStageStatus.SKIPPED,
            billing_at=now,
            datalake_status=ClosePipelineStageStatus.PENDING,
            datalake_pending_at=now,
        )

        # Topics already skipped — worker no-ops but Shape C skipped should still allow datalake
        run_topics_stage(str(conv.uuid))
        mock_enqueue_dl.assert_not_called()

        # First datalake run publishes both when topics already finished
        run_datalake_stage(str(conv.uuid))
        record.refresh_from_db()
        assert mock_send.call_count == 2
        assert record.datalake_classification_at is not None
        assert record.datalake_topics_at is not None
        assert record.datalake_status == ClosePipelineStageStatus.DONE


@pytest.mark.django_db
class TestBillingStage:
    @override_settings(SQS_BILLING_QUEUE_URL="")
    def test_empty_queue_url_marks_failed(self, project):
        conv = _open_conversation(project, resolution="0")
        now = timezone.now()
        ClosePipelineRecord.objects.create(
            conversation=conv,
            classify_status=ClosePipelineStageStatus.DONE,
            classify_at=now,
            topics_status=ClosePipelineStageStatus.SKIPPED,
            topics_at=now,
            billing_status=ClosePipelineStageStatus.PENDING,
            billing_pending_at=now,
            datalake_status=ClosePipelineStageStatus.PENDING,
            datalake_pending_at=now,
        )
        with pytest.raises(BillingConfigError):
            run_billing_stage(str(conv.uuid))
        record = ClosePipelineRecord.objects.get(conversation=conv)
        assert record.billing_status == ClosePipelineStageStatus.FAILED
        assert "SQS_BILLING_QUEUE_URL" in (record.billing_error or "")

    @override_settings(SQS_BILLING_QUEUE_URL="https://sqs.example/queue.fifo")
    @patch("conversation_ms.close_daily.stages.billing.get_billing_sqs_producer")
    def test_done_is_noop(self, mock_producer, project):
        conv = _open_conversation(project, resolution="0")
        now = timezone.now()
        ClosePipelineRecord.objects.create(
            conversation=conv,
            classify_status=ClosePipelineStageStatus.DONE,
            classify_at=now,
            topics_status=ClosePipelineStageStatus.SKIPPED,
            topics_at=now,
            billing_status=ClosePipelineStageStatus.DONE,
            billing_at=now,
            datalake_status=ClosePipelineStageStatus.PENDING,
            datalake_pending_at=now,
        )
        run_billing_stage(str(conv.uuid))
        mock_producer.assert_not_called()
