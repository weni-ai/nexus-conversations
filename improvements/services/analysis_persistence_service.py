from __future__ import annotations

from typing import Any

from improvements.enums import ImprovementRunStatus
from improvements.models import ImprovementAnalysisRun
from improvements.services.analysis_run_service import (
    mark_run_status,
    persist_analysis_batches,
    populate_run_conversations,
    update_run_s3_keys,
)
from improvements.services.improvements_check_service import (
    build_check_state_s3_key,
    upload_check_state_to_s3,
)
from improvements.services.improvements_json_builder import build_conversations_s3_key
from improvements.services.improvements_redbeat_service import TERMINAL_STATUSES
from improvements.services.improvements_state_ingest_service import ingest_improvements_state_data


def _sync_run_from_metadata_passthrough(
    run: ImprovementAnalysisRun,
    metadata_passthrough: dict[str, Any],
) -> None:
    update_fields: list[str] = []

    sampling_mode = metadata_passthrough.get("sampling_mode")
    if isinstance(sampling_mode, str) and sampling_mode and run.sampling_mode != sampling_mode:
        run.sampling_mode = sampling_mode
        update_fields.append("sampling_mode")

    sampling_metadata = metadata_passthrough.get("sampling_metadata")
    if isinstance(sampling_metadata, dict):
        population_n = sampling_metadata.get("population_N")
        if isinstance(population_n, int) and population_n > 0 and run.population_n != population_n:
            run.population_n = population_n
            update_fields.append("population_n")

    if update_fields:
        run.save(update_fields=update_fields)


def mark_run_building(run: ImprovementAnalysisRun) -> ImprovementAnalysisRun:
    return mark_run_status(run, ImprovementRunStatus.BUILDING)


def persist_analysis_build_phase(
    run: ImprovementAnalysisRun,
    *,
    payload: dict[str, Any],
    sample_size: int,
    conversation_uuids: list,
    analysis_result: dict[str, Any],
) -> ImprovementAnalysisRun:
    run.sample_size = sample_size
    run.save(update_fields=["sample_size"])
    populate_run_conversations(run, conversation_uuids)

    metadata_passthrough = analysis_result.get("metadata_passthrough")
    if isinstance(metadata_passthrough, dict):
        _sync_run_from_metadata_passthrough(run, metadata_passthrough)

    persist_analysis_batches(run, analysis_result["batches"])
    update_run_s3_keys(
        run,
        s3_build_key=build_conversations_s3_key(payload),
        s3_state_key=build_check_state_s3_key(
            str(payload["project_uuid"]),
            str(payload["target_date"]),
            str(run.uuid),
        ),
    )
    return mark_run_status(run, ImprovementRunStatus.POLLING)


def persist_analysis_check_result(
    run: ImprovementAnalysisRun | None,
    *,
    check_result: dict[str, Any],
    project_uuid: str,
    target_date: str,
    run_uuid: str,
) -> dict[str, Any] | None:
    state_data = check_result.get("state_data")
    if state_data is None:
        return None

    upload_check_state_to_s3(state_data, project_uuid, target_date, run_uuid)
    if run is None:
        return None

    check_status = check_result.get("status", "")
    return ingest_improvements_state_data(
        run,
        state_data,
        terminal=check_status in TERMINAL_STATUSES,
        check_result=check_result,
    )
