from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from django.db.models import Q
from django.utils.dateparse import parse_datetime

from improvements.enums import ImprovementItemStatus
from improvements.models import ImprovementBacklogItem
from improvements.services.improvements_list_service import (
    CUSTOM_ANALYSIS_TYPE,
    NATIVE_IMPROVEMENT_TYPES,
)
from improvements.services.project_customization_service import get_project_customization

logger = logging.getLogger(__name__)

MANAGER_INSTRUCTION_TARGET = "manager_instruction"

STATUS_TO_API: dict[str, str] = {
    ImprovementItemStatus.ACTIVE: "pending",
    ImprovementItemStatus.IGNORED: "ignored",
    ImprovementItemStatus.RESOLVED: "resolved",
}


class ImprovementDetailNotFound(Exception):
    pass


def _map_item_status(db_status: str) -> str:
    return STATUS_TO_API.get(db_status, "pending")


def _map_detail_type(dimension_id: str) -> str:
    if dimension_id.startswith("custom:"):
        return CUSTOM_ANALYSIS_TYPE
    return dimension_id


def get_backlog_item(
    project_uuid: UUID | str,
    improvement_uuid: UUID | str,
) -> ImprovementBacklogItem:
    item = (
        ImprovementBacklogItem.objects.filter(
            uuid=improvement_uuid,
            project_id=project_uuid,
        )
        .filter(
            Q(dimension_id__in=NATIVE_IMPROVEMENT_TYPES) | Q(dimension_id__startswith="custom:"),
        )
        .exclude(status=ImprovementItemStatus.SUPERSEDED)
        .first()
    )
    if item is None:
        raise ImprovementDetailNotFound
    return item


def _build_instructions_index(instructions: list[Any]) -> dict[int, dict[str, Any]]:
    index: dict[int, dict[str, Any]] = {}
    for entry in instructions:
        if not isinstance(entry, dict):
            continue
        instruction_id = entry.get("id")
        if isinstance(instruction_id, int):
            index[instruction_id] = entry
    return index


def _parse_instruction_updated_at(entry: dict[str, Any]) -> datetime | None:
    updated_at = entry.get("updated_at")
    if isinstance(updated_at, datetime):
        return updated_at
    if isinstance(updated_at, str):
        return parse_datetime(updated_at)
    return None


def _instruction_spec(
    instruction_id: int,
    change_type: str,
    *,
    snapshot_text: str | None = None,
) -> dict[str, Any]:
    return {
        "instruction_id": instruction_id,
        "change_type": change_type,
        "snapshot_text": snapshot_text,
    }


def _specs_from_contract_details(details: dict[str, Any]) -> list[dict[str, Any]] | None:
    change_type = str(details.get("instruction_change_type", "fix"))
    affected_ids = details.get("affected_instruction_ids")
    if not isinstance(affected_ids, list):
        return None

    specs = [
        _instruction_spec(instruction_id, change_type)
        for instruction_id in affected_ids
        if isinstance(instruction_id, int)
    ]
    if specs or change_type == "add":
        return specs
    return None


def _specs_from_legacy_instruction_refs(instruction_refs: list[Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for ref in instruction_refs:
        if not isinstance(ref, dict):
            continue
        instruction_id = ref.get("instruction_id")
        if not isinstance(instruction_id, int):
            continue
        specs.append(
            _instruction_spec(
                instruction_id,
                "fix",
                snapshot_text=ref.get("snapshot_text"),
            ),
        )
    return specs


def _is_non_manager_instruction_target(suggested_solution: dict[str, Any]) -> bool:
    target = suggested_solution.get("target")
    return bool(target and target != MANAGER_INSTRUCTION_TARGET)


def _has_manager_instruction_metadata(
    suggested_solution: dict[str, Any],
    details: Any,
) -> bool:
    if suggested_solution.get("target") == MANAGER_INSTRUCTION_TARGET:
        return True
    return isinstance(details, dict) and bool(details.get("instruction_change_type"))


def _extract_affected_instruction_specs(
    suggested_solution: dict[str, Any],
) -> list[dict[str, Any]]:
    if not isinstance(suggested_solution, dict):
        return []

    if _is_non_manager_instruction_target(suggested_solution):
        return []

    details = suggested_solution.get("details")
    if isinstance(details, dict):
        specs_from_details = _specs_from_contract_details(details)
        if specs_from_details is not None:
            return specs_from_details

    instruction_refs = suggested_solution.get("instruction_refs")
    if isinstance(instruction_refs, list):
        return _specs_from_legacy_instruction_refs(instruction_refs)

    if _has_manager_instruction_metadata(suggested_solution, details):
        return []

    return []


def _compute_was_changed(
    *,
    change_type: str,
    instruction_id: int,
    snapshot_text: str | None,
    current_instruction: dict[str, Any] | None,
    first_seen_at: datetime,
) -> bool:
    if change_type == "remove":
        return current_instruction is None

    if change_type == "add":
        return current_instruction is not None

    if current_instruction is None:
        return False

    if snapshot_text is not None:
        current_text = str(current_instruction.get("instruction", ""))
        return current_text != str(snapshot_text)

    updated_at = _parse_instruction_updated_at(current_instruction)
    if updated_at is not None and updated_at > first_seen_at:
        return True

    return False


def _build_affected_instructions(
    item: ImprovementBacklogItem,
    *,
    current_instructions: list[Any] | None,
) -> list[dict[str, Any]]:
    specs = _extract_affected_instruction_specs(item.suggested_solution or {})
    if not specs:
        return []

    instructions_index = _build_instructions_index(current_instructions or [])
    nexus_unavailable = current_instructions is None

    affected: list[dict[str, Any]] = []
    for spec in specs:
        instruction_id = spec["instruction_id"]
        change_type = spec["change_type"]
        current = instructions_index.get(instruction_id)

        if nexus_unavailable:
            was_changed = None
        else:
            was_changed = _compute_was_changed(
                change_type=change_type,
                instruction_id=instruction_id,
                snapshot_text=spec.get("snapshot_text"),
                current_instruction=current,
                first_seen_at=item.first_seen_at,
            )

        affected.append(
            {
                "instruction_id": instruction_id,
                "change_type": change_type,
                "was_changed": was_changed,
            },
        )

    return affected


def _fetch_current_instructions(project_uuid: UUID | str) -> list[Any] | None:
    try:
        customization = get_project_customization(str(project_uuid))
    except Exception:
        logger.exception(
            "[get_improvement_detail] Failed to fetch project customization project_uuid=%s",
            project_uuid,
        )
        return None

    instructions = customization.get("instructions") if isinstance(customization, dict) else None
    if isinstance(instructions, list):
        return instructions
    return []


def _extract_suggested_change(suggested_solution: dict[str, Any] | None) -> str | None:
    if not isinstance(suggested_solution, dict):
        return None
    suggested_change = suggested_solution.get("suggested_change")
    if suggested_change is None:
        return None
    return str(suggested_change)


def get_improvement_detail(
    project_uuid: UUID | str,
    improvement_uuid: UUID | str,
) -> dict[str, Any]:
    item = get_backlog_item(project_uuid, improvement_uuid)
    current_instructions = _fetch_current_instructions(project_uuid)
    suggested_solution = item.suggested_solution or {}

    return {
        "uuid": str(item.uuid),
        "text": item.title,
        "type": _map_detail_type(item.dimension_id),
        "description": item.diagnosis,
        "suggested_change": _extract_suggested_change(suggested_solution),
        "status": _map_item_status(item.status),
        "affected_instructions": _build_affected_instructions(
            item,
            current_instructions=current_instructions,
        ),
    }
