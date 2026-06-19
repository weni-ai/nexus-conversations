from django.db import models


class ImprovementRunStatus(models.TextChoices):
    QUEUED = "queued", "Queued"
    BUILDING = "building", "Building"
    POLLING = "polling", "Polling"
    IN_PROGRESS = "in_progress", "In progress"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"


class ImprovementItemStatus(models.TextChoices):
    ACTIVE = "active", "Active"
    IGNORED = "ignored", "Ignored"
    RESOLVED = "resolved", "Resolved"
    SUPERSEDED = "superseded", "Superseded"


class ImprovementDimensionId(models.TextChoices):
    BRAND_VOICE_MISMATCH = "brand_voice_mismatch", "Brand voice mismatch"
    MANY_QUESTIONS_BEFORE_ANSWERING = "many_questions_before_answering", "Many questions before answering"
    MISSING_STATIC_KNOWLEDGE = "missing_static_knowledge", "Missing static knowledge"
    INSTRUCTION_NON_COMPLIANCE = "instruction_non_compliance", "Instruction non compliance"
    CATALOG_SEARCH_MISMATCH = "catalog_search_mismatch", "Catalog search mismatch"


class ImprovementItemType(models.TextChoices):
    BEHAVIOR = "behavior", "Behavior"
    KNOWLEDGE = "knowledge", "Knowledge"
    TECHNICAL = "technical", "Technical"
    CUSTOM = "custom", "Custom"


class ImprovementConversationProcessingStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"


DIMENSION_TO_ITEM_TYPE: dict[str, str] = {
    ImprovementDimensionId.BRAND_VOICE_MISMATCH: ImprovementItemType.BEHAVIOR,
    ImprovementDimensionId.MANY_QUESTIONS_BEFORE_ANSWERING: ImprovementItemType.BEHAVIOR,
    ImprovementDimensionId.MISSING_STATIC_KNOWLEDGE: ImprovementItemType.KNOWLEDGE,
    ImprovementDimensionId.INSTRUCTION_NON_COMPLIANCE: ImprovementItemType.BEHAVIOR,
    ImprovementDimensionId.CATALOG_SEARCH_MISMATCH: ImprovementItemType.TECHNICAL,
}

MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT = 10


def resolve_item_type(dimension_id: str) -> str:
    if dimension_id.startswith("custom:"):
        return ImprovementItemType.CUSTOM
    return DIMENSION_TO_ITEM_TYPE.get(dimension_id, ImprovementItemType.BEHAVIOR)
