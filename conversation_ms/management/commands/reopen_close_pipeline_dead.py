"""Ops: bulk reopen close-pipeline stages from dead → pending (reset reclaim budget)."""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from conversation_ms.close_daily.constants import CLOSE_PIPELINE_STAGES, ClosePipelineStageStatus
from conversation_ms.close_daily.enqueue import (
    enqueue_billing,
    enqueue_classify,
    enqueue_datalake,
    enqueue_topics,
)
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord


def _enqueue_for_stage(stage: str):
    return {
        "classify": enqueue_classify,
        "topics": enqueue_topics,
        "billing": enqueue_billing,
        "datalake": enqueue_datalake,
    }[stage]


class Command(BaseCommand):
    help = (
        "Bulk reclaim ClosePipelineRecord stages from dead → pending "
        "(resets reclaim_count). Optionally enqueue the stage worker."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--stage",
            required=True,
            choices=list(CLOSE_PIPELINE_STAGES),
            help="Pipeline stage to reopen",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Max rows to reopen (default 100)",
        )
        parser.add_argument(
            "--error-contains",
            default="",
            help="Optional substring filter on {stage}_error",
        )
        parser.add_argument(
            "--enqueue",
            action="store_true",
            help="Enqueue the stage Celery task after reclaim",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List matching rows without mutating",
        )

    def handle(self, *args, **options):
        stage = options["stage"]
        limit = options["limit"]
        if limit < 1:
            raise CommandError("--limit must be >= 1")

        status_field = f"{stage}_status"
        error_field = f"{stage}_error"
        qs = ClosePipelineRecord.objects.filter(**{status_field: ClosePipelineStageStatus.DEAD}).order_by(
            "created_at", "conversation_id"
        )
        needle = (options.get("error_contains") or "").strip()
        if needle:
            qs = qs.filter(**{f"{error_field}__icontains": needle})

        rows = list(qs[:limit])
        self.stdout.write(
            f"matched={len(rows)} stage={stage} dry_run={options['dry_run']} enqueue={options['enqueue']}"
        )
        if options["dry_run"]:
            for record in rows:
                self.stdout.write(f"  conversation={record.conversation_id} error={getattr(record, error_field)!r}")
            return

        reopened = 0
        for record in rows:
            updated = ClosePipelineStateMachine.reclaim_dead(record, stage)
            if options["enqueue"]:
                _enqueue_for_stage(stage)(str(updated.conversation_id))
            reopened += 1
            self.stdout.write(f"  reopened conversation={updated.conversation_id}")

        self.stdout.write(self.style.SUCCESS(f"reopened={reopened} stage={stage}"))
