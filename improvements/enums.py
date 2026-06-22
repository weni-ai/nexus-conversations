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


class ImprovementProblemType(models.TextChoices):
    MANY_QUESTIONS_BEFORE_ANSWERING = (
        "many_questions_before_answering",
        "Many questions before answering",
    )
    WRONG_BEHAVIOR_DUE_TO_INSTRUCTIONS = (
        "wrong_behavior_due_to_instructions",
        "Wrong behavior due to instructions",
    )
    MISSING_STATIC_KNOWLEDGE = "missing_static_knowledge", "Missing static knowledge"
    PERSONALITY_DEVIATION = "personality_deviation", "Personality deviation"
    MENTIONS_COMPETITORS = "mentions_competitors", "Mentions competitors"
    POOR_PRODUCT_SEARCH_RESULTS = (
        "poor_product_search_results",
        "Poor product search results",
    )
    REPETITIVE_RESPONSE = "repetitive_response", "Repetitive response"


# Backward-compatible alias for code that still references ImprovementDimensionId.
ImprovementDimensionId = ImprovementProblemType

PROBLEM_TYPES_EXCLUDED_FROM_BACKLOG = frozenset(
    {
        "none",
        "unclear",
        "amazing_conversations",
    }
)


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
    ImprovementProblemType.MANY_QUESTIONS_BEFORE_ANSWERING: ImprovementItemType.BEHAVIOR,
    ImprovementProblemType.WRONG_BEHAVIOR_DUE_TO_INSTRUCTIONS: ImprovementItemType.BEHAVIOR,
    ImprovementProblemType.MISSING_STATIC_KNOWLEDGE: ImprovementItemType.KNOWLEDGE,
    ImprovementProblemType.PERSONALITY_DEVIATION: ImprovementItemType.BEHAVIOR,
    ImprovementProblemType.MENTIONS_COMPETITORS: ImprovementItemType.BEHAVIOR,
    ImprovementProblemType.POOR_PRODUCT_SEARCH_RESULTS: ImprovementItemType.TECHNICAL,
    ImprovementProblemType.REPETITIVE_RESPONSE: ImprovementItemType.BEHAVIOR,
}

MAX_ACTIVE_CUSTOM_MONITORS_PER_PROJECT = 10


def resolve_item_type(dimension_id: str) -> str:
    if dimension_id.startswith("custom:"):
        return ImprovementItemType.CUSTOM
    return DIMENSION_TO_ITEM_TYPE.get(dimension_id, ImprovementItemType.BEHAVIOR)
