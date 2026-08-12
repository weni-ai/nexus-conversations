"""Ops: bulk reopen close-pipeline stages from dead → pending (reset reclaim budget)."""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from conversation_ms.close_daily.constants import (
    CLOSE_PIPELINE_DRAIN_BATCH_SIZE_DEFAULT,
    CLOSE_PIPELINE_STAGES,
    ClosePipelineStageStatus,
)
from conversation_ms.close_daily.enqueue import enqueue_stage
from conversation_ms.close_daily.state_machine import ClosePipelineStateMachine
from conversation_ms.models import ClosePipelineRecord


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
            default=CLOSE_PIPELINE_DRAIN_BATCH_SIZE_DEFAULT,
            help=f"Max rows to reopen (default {CLOSE_PIPELINE_DRAIN_BATCH_SIZE_DEFAULT})",
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
                enqueue_stage(stage, str(updated.conversation_id))
            reopened += 1
            self.stdout.write(f"  reopened conversation={updated.conversation_id}")

        self.stdout.write(self.style.SUCCESS(f"reopened={reopened} stage={stage}"))
