"""Close-daily pipeline stage transitions (make unreasonable states invalid)."""

from __future__ import annotations

from django.db import IntegrityError, transaction
from django.utils import timezone

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_STAGES,
    RESOLUTION_IN_PROGRESS,
    TERMINAL_RESOLUTIONS,
    ClosePipelineStageStatus,
)
from conversation_ms.models import CloseDatalakeOutbox, ClosePipelineRecord, Conversation


class InvalidClosePipelineTransition(Exception):
    """Raised when a stage transition is not allowed."""


class InvalidClosePipelineData(Exception):
    """Raised when transition payload fails validation before persisting."""


class ClosePipelineStateMachine:
    """
    Only legal writer of ``ClosePipelineRecord`` stage fields.

    Per-stage graph::

        NULL → pending → done
                   ├→ skipped ⇄ pending   (ops reclaim only)
                   ├→ failed  → pending   (drain / ops; may consume reclaim budget)
                   └→ failed|pending → dead (drain when reclaim budget exhausted)
        dead → pending                 (ops only; resets reclaim_count)
    """

    _OPS_RECLAIM_SKIPPED_STAGES = frozenset({"topics", "billing", "datalake"})

    @classmethod
    def _locked_record(cls, record: ClosePipelineRecord) -> ClosePipelineRecord:
        return ClosePipelineRecord.objects.select_for_update().get(pk=record.pk)

    @classmethod
    def _locked_conversation(cls, conversation: Conversation) -> Conversation:
        return Conversation.objects.select_for_update().get(pk=conversation.pk)

    @staticmethod
    def _require_stage(stage: str) -> None:
        if stage not in CLOSE_PIPELINE_STAGES:
            raise InvalidClosePipelineData(f"Unknown close pipeline stage: {stage!r}")

    @classmethod
    def _status_field(cls, stage: str) -> str:
        return f"{stage}_status"

    @classmethod
    def _at_field(cls, stage: str) -> str:
        return f"{stage}_at"

    @classmethod
    def _pending_at_field(cls, stage: str) -> str:
        return f"{stage}_pending_at"

    @classmethod
    def _error_field(cls, stage: str) -> str:
        return f"{stage}_error"

    @classmethod
    def _reclaim_count_field(cls, stage: str) -> str:
        return f"{stage}_reclaim_count"

    @classmethod
    def _get_status(cls, record: ClosePipelineRecord, stage: str) -> str | None:
        return getattr(record, cls._status_field(stage))

    @classmethod
    def _enter_pending(cls, record: ClosePipelineRecord, stage: str, *, now) -> list[str]:
        setattr(record, cls._status_field(stage), ClosePipelineStageStatus.PENDING)
        setattr(record, cls._at_field(stage), None)
        setattr(record, cls._pending_at_field(stage), now)
        setattr(record, cls._error_field(stage), None)
        return [
            cls._status_field(stage),
            cls._at_field(stage),
            cls._pending_at_field(stage),
            cls._error_field(stage),
        ]

    @classmethod
    def _leave_pending_to_finished(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        status: str,
        now,
    ) -> list[str]:
        if status not in ClosePipelineStageStatus.FINISHED:
            raise InvalidClosePipelineData(f"Finished status must be done/skipped, got {status!r}")
        setattr(record, cls._status_field(stage), status)
        setattr(record, cls._at_field(stage), now)
        setattr(record, cls._pending_at_field(stage), None)
        setattr(record, cls._error_field(stage), None)
        return [
            cls._status_field(stage),
            cls._at_field(stage),
            cls._pending_at_field(stage),
            cls._error_field(stage),
        ]

    @classmethod
    def _leave_to_failed_or_dead(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        status: str,
        error: str,
    ) -> list[str]:
        if status not in {ClosePipelineStageStatus.FAILED, ClosePipelineStageStatus.DEAD}:
            raise InvalidClosePipelineData(f"Error status must be failed/dead, got {status!r}")
        if not error or not str(error).strip():
            raise InvalidClosePipelineData(f"{status} status requires a non-empty error")
        setattr(record, cls._status_field(stage), status)
        setattr(record, cls._at_field(stage), None)
        setattr(record, cls._pending_at_field(stage), None)
        setattr(record, cls._error_field(stage), str(error).strip())
        return [
            cls._status_field(stage),
            cls._at_field(stage),
            cls._pending_at_field(stage),
            cls._error_field(stage),
        ]

    @classmethod
    def _leave_pending_to_failed(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        error: str,
    ) -> list[str]:
        return cls._leave_to_failed_or_dead(
            record,
            stage,
            status=ClosePipelineStageStatus.FAILED,
            error=error,
        )

    @classmethod
    @transaction.atomic
    def claim_classify(cls, conversation: Conversation) -> ClosePipelineRecord | None:
        """
        Shape A → B: insert pipeline record with classify pending.

        Returns the new record, or ``None`` if already claimed / not eligible.
        """
        conversation = cls._locked_conversation(conversation)
        if conversation.resolution != RESOLUTION_IN_PROGRESS:
            return None
        if ClosePipelineRecord.objects.filter(conversation_id=conversation.pk).exists():
            return None

        now = timezone.now()
        try:
            record = ClosePipelineRecord(
                conversation=conversation,
                classify_status=ClosePipelineStageStatus.PENDING,
                classify_pending_at=now,
            )
            record.save()
        except IntegrityError:
            return None
        return record

    @classmethod
    @transaction.atomic
    def fail_classify(cls, record: ClosePipelineRecord, error: str) -> ClosePipelineRecord:
        record = cls._locked_record(record)
        status = record.classify_status
        if status == ClosePipelineStageStatus.FAILED:
            return record
        if status != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineTransition(f"Cannot fail classify from status {status!r}")
        update_fields = cls._leave_pending_to_failed(record, "classify", error=error)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def commit_classify_success(
        cls,
        record: ClosePipelineRecord,
        *,
        resolution: str,
        topics_status: str = ClosePipelineStageStatus.PENDING,
        billing_status: str = ClosePipelineStageStatus.PENDING,
        datalake_status: str = ClosePipelineStageStatus.PENDING,
    ) -> ClosePipelineRecord:
        """
        Atomic Shape B → C: set terminal resolution and initialize downstream stages.
        """
        if resolution not in TERMINAL_RESOLUTIONS:
            raise InvalidClosePipelineData(f"commit_classify_success requires terminal resolution, got {resolution!r}")
        if topics_status not in {
            ClosePipelineStageStatus.PENDING,
            ClosePipelineStageStatus.SKIPPED,
        }:
            raise InvalidClosePipelineData(f"topics_status at Shape C must be pending/skipped, got {topics_status!r}")
        if billing_status not in {
            ClosePipelineStageStatus.PENDING,
            ClosePipelineStageStatus.SKIPPED,
        }:
            raise InvalidClosePipelineData(f"billing_status at Shape C must be pending/skipped, got {billing_status!r}")
        if datalake_status != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineData(f"datalake_status at Shape C must be pending in v1, got {datalake_status!r}")

        record = cls._locked_record(record)
        conversation = cls._locked_conversation(record.conversation)

        if record.classify_status != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineTransition(f"Cannot commit classify from status {record.classify_status!r}")
        if conversation.resolution != RESOLUTION_IN_PROGRESS:
            raise InvalidClosePipelineTransition(
                f"Cannot commit classify while resolution is {conversation.resolution!r}"
            )

        now = timezone.now()
        conversation.resolution = resolution
        conversation.save(update_fields=["resolution"])

        update_fields = cls._leave_pending_to_finished(
            record,
            "classify",
            status=ClosePipelineStageStatus.DONE,
            now=now,
        )

        for stage, init_status in (
            ("topics", topics_status),
            ("billing", billing_status),
            ("datalake", datalake_status),
        ):
            if init_status == ClosePipelineStageStatus.PENDING:
                update_fields.extend(cls._enter_pending(record, stage, now=now))
            else:
                # Shape C may initialize a stage directly to skipped (never pending).
                update_fields.extend(cls._leave_pending_to_finished(record, stage, status=init_status, now=now))

        record.datalake_classification_at = None
        record.datalake_topics_at = None
        update_fields.extend(["datalake_classification_at", "datalake_topics_at"])

        record.save(update_fields=sorted(set(update_fields)))
        return record

    @classmethod
    @transaction.atomic
    def heartbeat_pending(cls, record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
        cls._require_stage(stage)
        record = cls._locked_record(record)
        if cls._get_status(record, stage) != ClosePipelineStageStatus.PENDING:
            return record
        now = timezone.now()
        setattr(record, cls._pending_at_field(stage), now)
        record.save(update_fields=[cls._pending_at_field(stage)])
        return record

    @classmethod
    @transaction.atomic
    def mark_done(cls, record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
        return cls._mark_finished(record, stage, status=ClosePipelineStageStatus.DONE)

    @classmethod
    @transaction.atomic
    def mark_skipped(cls, record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
        return cls._mark_finished(record, stage, status=ClosePipelineStageStatus.SKIPPED)

    @classmethod
    def _mark_finished(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        status: str,
    ) -> ClosePipelineRecord:
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current in ClosePipelineStageStatus.FINISHED and current == status:
            return record
        if current in ClosePipelineStageStatus.FINISHED:
            return record  # idempotent no-op for other finished
        if current != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineTransition(f"Cannot mark {stage} {status} from status {current!r}")
        now = timezone.now()
        update_fields = cls._leave_pending_to_finished(record, stage, status=status, now=now)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def mark_failed(cls, record: ClosePipelineRecord, stage: str, error: str) -> ClosePipelineRecord:
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current == ClosePipelineStageStatus.FAILED:
            return record
        if current in ClosePipelineStageStatus.FINISHED:
            return record  # hard no-op
        if current != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineTransition(f"Cannot mark {stage} failed from status {current!r}")
        update_fields = cls._leave_pending_to_failed(record, stage, error=error)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def mark_dead(cls, record: ClosePipelineRecord, stage: str, error: str) -> ClosePipelineRecord:
        """Drain: failed|pending → dead (logical dead letter; no enqueue)."""
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current == ClosePipelineStageStatus.DEAD:
            return record
        if current not in {ClosePipelineStageStatus.FAILED, ClosePipelineStageStatus.PENDING}:
            raise InvalidClosePipelineTransition(f"Cannot mark {stage} dead from status {current!r}")
        update_fields = cls._leave_to_failed_or_dead(
            record,
            stage,
            status=ClosePipelineStageStatus.DEAD,
            error=error,
        )
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def reclaim_failed(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        consume_budget: bool = True,
    ) -> ClosePipelineRecord:
        """
        Drain / ops: failed → pending.
        """
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current == ClosePipelineStageStatus.PENDING:
            return record
        if current != ClosePipelineStageStatus.FAILED:
            raise InvalidClosePipelineTransition(f"Cannot reclaim {stage} from status {current!r} (expected failed)")
        now = timezone.now()
        update_fields = cls._enter_pending(record, stage, now=now)
        if consume_budget:
            count_field = cls._reclaim_count_field(stage)
            setattr(record, count_field, getattr(record, count_field) + 1)
            update_fields.append(count_field)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def reclaim_stale_pending(
        cls,
        record: ClosePipelineRecord,
        stage: str,
        *,
        consume_budget: bool = True,
    ) -> ClosePipelineRecord:
        """
        Drain: stale pending → pending with fresh ``pending_at``.

        When ``consume_budget`` is True (default), increments ``{stage}_reclaim_count``.
        """
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current != ClosePipelineStageStatus.PENDING:
            raise InvalidClosePipelineTransition(
                f"Cannot reclaim stale pending {stage} from status {current!r} (expected pending)"
            )
        now = timezone.now()
        setattr(record, cls._pending_at_field(stage), now)
        update_fields = [cls._pending_at_field(stage)]
        if consume_budget:
            count_field = cls._reclaim_count_field(stage)
            setattr(record, count_field, getattr(record, count_field) + 1)
            update_fields.append(count_field)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def reclaim_dead(cls, record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
        """Ops-only: dead → pending and reset reclaim_count to 0."""
        cls._require_stage(stage)
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current == ClosePipelineStageStatus.PENDING:
            return record
        if current != ClosePipelineStageStatus.DEAD:
            raise InvalidClosePipelineTransition(f"Cannot reclaim dead {stage} from status {current!r}")
        now = timezone.now()
        update_fields = cls._enter_pending(record, stage, now=now)
        count_field = cls._reclaim_count_field(stage)
        setattr(record, count_field, 0)
        update_fields.append(count_field)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def reclaim_skipped(cls, record: ClosePipelineRecord, stage: str) -> ClosePipelineRecord:
        """Ops-only: skipped → pending for topics/billing/datalake."""
        cls._require_stage(stage)
        if stage not in cls._OPS_RECLAIM_SKIPPED_STAGES:
            raise InvalidClosePipelineTransition(f"Ops skipped→pending is not allowed for stage {stage!r}")
        record = cls._locked_record(record)
        current = cls._get_status(record, stage)
        if current == ClosePipelineStageStatus.PENDING:
            return record
        if current != ClosePipelineStageStatus.SKIPPED:
            raise InvalidClosePipelineTransition(f"Cannot reclaim skipped {stage} from status {current!r}")
        now = timezone.now()
        update_fields = cls._enter_pending(record, stage, now=now)
        record.save(update_fields=update_fields)
        return record

    @classmethod
    @transaction.atomic
    def abandon_pipeline(
        cls,
        record: ClosePipelineRecord,
        *,
        resolution: str | None = None,
    ) -> Conversation:
        """
        Ops-only: delete the pipeline record (± set resolution).
        """
        if resolution is not None and resolution not in TERMINAL_RESOLUTIONS:
            raise InvalidClosePipelineData(f"abandon_pipeline resolution must be terminal or None, got {resolution!r}")

        record = cls._locked_record(record)
        conversation = cls._locked_conversation(record.conversation)
        conversation_id = conversation.pk

        CloseDatalakeOutbox.objects.filter(conversation_id=conversation_id).delete()
        record.delete()

        if resolution is not None:
            conversation.resolution = resolution
            conversation.save(update_fields=["resolution"])

        return conversation

    @classmethod
    @transaction.atomic
    def mark_datalake_event_sent(
        cls,
        record: ClosePipelineRecord,
        *,
        event: str,
    ) -> ClosePipelineRecord:
        """
        Set a datalake event timestamp once. When both are set, promote datalake to done.
        """
        if event not in {"classification", "topics"}:
            raise InvalidClosePipelineData(f"Unknown datalake event {event!r}")

        record = cls._locked_record(record)
        if record.datalake_status in ClosePipelineStageStatus.FINISHED:
            return record
        if record.datalake_status not in {
            ClosePipelineStageStatus.PENDING,
            ClosePipelineStageStatus.FAILED,
        }:
            raise InvalidClosePipelineTransition(f"Cannot mark datalake event from status {record.datalake_status!r}")

        field = "datalake_classification_at" if event == "classification" else "datalake_topics_at"
        now = timezone.now()
        update_fields = [field]
        if getattr(record, field) is None:
            setattr(record, field, now)

        if record.datalake_classification_at is not None and record.datalake_topics_at is not None:
            record.datalake_status = ClosePipelineStageStatus.DONE
            record.datalake_at = now
            record.datalake_pending_at = None
            record.datalake_error = None
            update_fields.extend(["datalake_status", "datalake_at", "datalake_pending_at", "datalake_error"])

        record.save(update_fields=sorted(set(update_fields)))
        return record
