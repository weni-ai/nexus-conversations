from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import pendulum
from django.conf import settings
from django.core.exceptions import ValidationError

from conversation_ms.models import Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum, parse_api_utc
from conversation_ms.utils.date_helpers import resolve_effective_project_timezone
from improvements.enums import (
    ImprovementConversationProcessingStatus,
    ImprovementRunStatus,
)
from improvements.models import (
    ImprovementAnalysisBatch,
    ImprovementAnalysisRun,
    ImprovementRunConversation,
)
from improvements.utils.time import parse_to_django_utc, utc_now


class AnalysisRunAlreadyExistsError(Exception):
    """Raised when a run already exists for the project on the given calendar day."""


BUILDING_TIMEOUT_FAILURE_REASON = "building_timeout"


def _parse_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return None


def resolve_triggered_on_date(project: Project) -> str:
    tz = resolve_effective_project_timezone(project.timezone)
    return pendulum.now(tz).format("YYYY-MM-DD")


def create_analysis_run(
    project: Project,
    *,
    payload: dict[str, Any],
    triggered_by_actor: str | None = None,
) -> ImprovementAnalysisRun:
    triggered_on_date = resolve_triggered_on_date(project)
    if ImprovementAnalysisRun.objects.filter(
        project=project,
        triggered_on_date=triggered_on_date,
    ).exists():
        raise AnalysisRunAlreadyExistsError(
            f"An analysis run already exists for project={project.uuid} on {triggered_on_date}",
        )

    start_utc = parse_api_utc(str(payload["start"]))
    end_utc = parse_api_utc(str(payload["end"]))

    return ImprovementAnalysisRun.objects.create(
        project=project,
        target_date=payload["target_date"],
        triggered_on_date=triggered_on_date,
        status=ImprovementRunStatus.QUEUED,
        sampling_mode=str(payload.get("sampling_mode", "srs")),
        population_n=int(payload.get("total_count", 0)),
        range_start_utc=django_utc_from_pendulum(start_utc),
        range_end_utc=django_utc_from_pendulum(end_utc),
        triggered_by_actor=triggered_by_actor,
    )


def create_analysis_run_from_payload(payload: dict[str, Any]) -> ImprovementAnalysisRun:
    project_uuid = _parse_uuid(payload.get("project_uuid"))
    if project_uuid is None:
        raise ValueError("payload must include a valid project_uuid to create an analysis run")

    project = Project.objects.filter(uuid=project_uuid).first()
    if project is None:
        raise ValueError(f"Project not found for project_uuid={project_uuid}")

    return create_analysis_run(project, payload=payload)


def get_analysis_run_for_payload(payload: dict[str, Any]) -> ImprovementAnalysisRun | None:
    run_uuid = _parse_uuid(payload.get("run_uuid"))
    if run_uuid is not None:
        return ImprovementAnalysisRun.objects.filter(uuid=run_uuid).first()

    project_uuid = _parse_uuid(payload.get("project_uuid"))
    target_date = payload.get("target_date")
    if project_uuid is None or not target_date:
        return None

    try:
        return (
            ImprovementAnalysisRun.objects.filter(
                project_id=project_uuid,
                target_date=target_date,
            )
            .order_by("-started_at")
            .first()
        )
    except ValidationError:
        return None


def get_analysis_run_by_project_and_target_date(
    project_uuid: str,
    target_date: str,
) -> ImprovementAnalysisRun | None:
    parsed_project_uuid = _parse_uuid(project_uuid)
    if parsed_project_uuid is None:
        return None
    try:
        return (
            ImprovementAnalysisRun.objects.filter(
                project_id=parsed_project_uuid,
                target_date=target_date,
            )
            .order_by("-started_at")
            .first()
        )
    except ValidationError:
        return None


def mark_run_status(
    run: ImprovementAnalysisRun,
    status: str,
    *,
    failure_reason: str | None = None,
) -> ImprovementAnalysisRun:
    run.status = status
    if failure_reason is not None:
        run.failure_reason = failure_reason
    if status in {ImprovementRunStatus.COMPLETED, ImprovementRunStatus.FAILED, ImprovementRunStatus.CANCELLED}:
        run.completed_at = utc_now()
    run.save(
        update_fields=[
            "status",
            "failure_reason",
            "completed_at",
        ],
    )
    return run


def fail_stale_building_runs(*, older_than_seconds: int | None = None) -> int:
    """Mark BUILDING runs older than the timeout as FAILED. Returns how many were expired."""
    timeout = older_than_seconds
    if timeout is None:
        timeout = int(getattr(settings, "IMPROVEMENTS_BUILDING_TIMEOUT_SECONDS", 2700))
    cutoff = utc_now() - timedelta(seconds=timeout)
    stale_runs = ImprovementAnalysisRun.objects.filter(
        status=ImprovementRunStatus.BUILDING,
        started_at__lt=cutoff,
    )
    expired = 0
    for run in stale_runs.iterator():
        mark_run_status(run, ImprovementRunStatus.FAILED, failure_reason=BUILDING_TIMEOUT_FAILURE_REASON)
        expired += 1
    return expired


def populate_run_conversations(run: ImprovementAnalysisRun, conversation_uuids: list[UUID | str]) -> int:
    if not conversation_uuids:
        run.conversations_total = 0
        run.save(update_fields=["conversations_total"])
        return 0

    existing = set(
        ImprovementRunConversation.objects.filter(run=run).values_list("conversation_id", flat=True),
    )
    to_create = []
    for conversation_uuid in conversation_uuids:
        conv_uuid = str(conversation_uuid)
        if conv_uuid in existing:
            continue
        to_create.append(
            ImprovementRunConversation(
                run=run,
                conversation_id=conv_uuid,
                processing_status=ImprovementConversationProcessingStatus.PENDING,
            ),
        )

    if to_create:
        ImprovementRunConversation.objects.bulk_create(to_create, ignore_conflicts=True)

    total = ImprovementRunConversation.objects.filter(run=run).count()
    run.conversations_total = total
    run.save(update_fields=["conversations_total"])
    return total


def persist_analysis_batches(run: ImprovementAnalysisRun, batches: list[dict[str, Any]]) -> None:
    ImprovementAnalysisBatch.objects.filter(run=run).delete()
    rows = [
        ImprovementAnalysisBatch(
            run=run,
            batch_id=str(batch["batch_id"]),
            input_file_id=str(batch["input_file_id"]),
            endpoint=str(batch["endpoint"]),
            n_requests=int(batch["n_requests"]),
            submitted_at=parse_to_django_utc(batch.get("submitted_at")),
            position=index,
        )
        for index, batch in enumerate(batches)
    ]
    if rows:
        ImprovementAnalysisBatch.objects.bulk_create(rows)


def update_run_s3_keys(
    run: ImprovementAnalysisRun,
    *,
    s3_build_key: str | None = None,
    s3_state_key: str | None = None,
) -> None:
    update_fields: list[str] = []
    if s3_build_key is not None:
        run.s3_build_key = s3_build_key
        update_fields.append("s3_build_key")
    if s3_state_key is not None:
        run.s3_state_key = s3_state_key
        update_fields.append("s3_state_key")
    if update_fields:
        run.save(update_fields=update_fields)


def sync_run_cancel_requested(run: ImprovementAnalysisRun, *, cancel_requested: bool) -> None:
    if run.cancel_requested == cancel_requested:
        return
    run.cancel_requested = cancel_requested
    run.save(update_fields=["cancel_requested"])
