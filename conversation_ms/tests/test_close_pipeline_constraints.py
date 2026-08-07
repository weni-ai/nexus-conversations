"""DB constraints for ClosePipelineRecord and CloseDatalakeOutbox."""

import importlib
from datetime import timedelta
from uuid import uuid4

import pytest
from django.db import IntegrityError
from django.utils import timezone

from conversation_ms.close_daily.constants import CloseDatalakeEventKind, ClosePipelineStageStatus
from conversation_ms.models import CloseDatalakeOutbox, ClosePipelineRecord, Conversation

_backfill_module = importlib.import_module("conversation_ms.migrations.0007_close_pipeline_record")
backfill_legacy_close_pipeline_records = _backfill_module.backfill_legacy_close_pipeline_records


def _conversation(project, *, resolution="2"):
    return Conversation.objects.create(
        project=project,
        contact_urn="whatsapp:+5511999999999",
        contact_name="Constraint Contact",
        channel_uuid=uuid4(),
        resolution=resolution,
        end_date=timezone.now(),
    )


def _all_done_kwargs(*, stamped_at=None):
    stamped_at = stamped_at or timezone.now()
    return {
        "classify_status": ClosePipelineStageStatus.DONE,
        "classify_at": stamped_at,
        "topics_status": ClosePipelineStageStatus.DONE,
        "topics_at": stamped_at,
        "billing_status": ClosePipelineStageStatus.DONE,
        "billing_at": stamped_at,
        "datalake_status": ClosePipelineStageStatus.DONE,
        "datalake_at": stamped_at,
        "datalake_classification_at": stamped_at,
        "datalake_topics_at": stamped_at,
    }


@pytest.mark.django_db
class TestClosePipelineShapeConstraints:
    def test_pending_requires_pending_at(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.PENDING,
            )

    def test_done_requires_at(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.DONE,
            )

    def test_failed_requires_non_empty_error(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.FAILED,
                classify_error="",
            )

    def test_dead_requires_non_empty_error(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.DEAD,
                classify_error="",
            )

    def test_valid_dead_shape(self, project):
        conversation = _conversation(project)
        record = ClosePipelineRecord.objects.create(
            conversation=conversation,
            classify_status=ClosePipelineStageStatus.DEAD,
            classify_error="reclaim budget exhausted",
            classify_reclaim_count=5,
        )
        assert record.classify_status == ClosePipelineStageStatus.DEAD
        assert record.classify_pending_at is None
        assert record.classify_at is None

    def test_valid_pending_shape(self, project):
        conversation = _conversation(project)
        record = ClosePipelineRecord.objects.create(
            conversation=conversation,
            classify_status=ClosePipelineStageStatus.PENDING,
            classify_pending_at=timezone.now(),
        )
        assert record.classify_status == ClosePipelineStageStatus.PENDING
        assert record.topics_status is None

    def test_downstream_requires_classify_finished(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.PENDING,
                classify_pending_at=timezone.now(),
                topics_status=ClosePipelineStageStatus.PENDING,
                topics_pending_at=timezone.now(),
            )

    def test_classify_finished_requires_downstream(self, project):
        conversation = _conversation(project)
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.DONE,
                classify_at=timezone.now(),
            )

    def test_valid_shape_c_pending_downstream(self, project):
        conversation = _conversation(project, resolution="0")
        now = timezone.now()
        record = ClosePipelineRecord.objects.create(
            conversation=conversation,
            classify_status=ClosePipelineStageStatus.DONE,
            classify_at=now,
            topics_status=ClosePipelineStageStatus.PENDING,
            topics_pending_at=now,
            billing_status=ClosePipelineStageStatus.PENDING,
            billing_pending_at=now,
            datalake_status=ClosePipelineStageStatus.PENDING,
            datalake_pending_at=now,
        )
        assert record.datalake_classification_at is None

    def test_both_datalake_events_require_finished_datalake(self, project):
        conversation = _conversation(project, resolution="0")
        now = timezone.now()
        with pytest.raises(IntegrityError):
            ClosePipelineRecord.objects.create(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.DONE,
                classify_at=now,
                topics_status=ClosePipelineStageStatus.DONE,
                topics_at=now,
                billing_status=ClosePipelineStageStatus.DONE,
                billing_at=now,
                datalake_status=ClosePipelineStageStatus.PENDING,
                datalake_pending_at=now,
                datalake_classification_at=now,
                datalake_topics_at=now,
            )

    def test_terminal_without_record_is_allowed_shape_e(self, project):
        conversation = _conversation(project, resolution="0")
        assert not ClosePipelineRecord.objects.filter(conversation=conversation).exists()


@pytest.mark.django_db
class TestCloseDatalakeOutboxConstraints:
    def test_unique_conversation_event_kind(self, project):
        conversation = _conversation(project, resolution="0")
        CloseDatalakeOutbox.objects.create(
            conversation=conversation,
            event_kind=CloseDatalakeEventKind.CLASSIFICATION,
        )
        with pytest.raises(IntegrityError):
            CloseDatalakeOutbox.objects.create(
                conversation=conversation,
                event_kind=CloseDatalakeEventKind.CLASSIFICATION,
            )

    def test_allows_both_event_kinds(self, project):
        conversation = _conversation(project, resolution="0")
        CloseDatalakeOutbox.objects.create(
            conversation=conversation,
            event_kind=CloseDatalakeEventKind.CLASSIFICATION,
        )
        CloseDatalakeOutbox.objects.create(
            conversation=conversation,
            event_kind=CloseDatalakeEventKind.TOPICS,
        )
        assert CloseDatalakeOutbox.objects.filter(conversation=conversation).count() == 2


@pytest.mark.django_db
class TestLegacyBackfill:
    def test_backfill_stamps_terminal_all_done(self, project):
        from django.apps import apps

        terminal = _conversation(project, resolution="0")
        in_progress = _conversation(project, resolution="2")

        backfill_legacy_close_pipeline_records(apps, None)

        record = ClosePipelineRecord.objects.get(conversation=terminal)
        assert record.classify_status == ClosePipelineStageStatus.DONE
        assert record.topics_status == ClosePipelineStageStatus.DONE
        assert record.billing_status == ClosePipelineStageStatus.DONE
        assert record.datalake_status == ClosePipelineStageStatus.DONE
        assert record.classify_reclaim_count == 0
        assert record.topics_reclaim_count == 0
        assert record.billing_reclaim_count == 0
        assert record.datalake_reclaim_count == 0
        assert record.datalake_classification_at is not None
        assert record.datalake_topics_at is not None
        assert not ClosePipelineRecord.objects.filter(conversation=in_progress).exists()

    def test_backfill_is_idempotent(self, project):
        from django.apps import apps

        terminal = _conversation(project, resolution="1")
        backfill_legacy_close_pipeline_records(apps, None)
        first_at = ClosePipelineRecord.objects.get(conversation=terminal).classify_at
        later = first_at + timedelta(hours=1)
        ClosePipelineRecord.objects.filter(conversation=terminal).update(classify_at=later)

        backfill_legacy_close_pipeline_records(apps, None)
        record = ClosePipelineRecord.objects.get(conversation=terminal)
        assert record.classify_at == later
