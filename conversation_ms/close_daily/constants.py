CLOSE_DAILY_PROJECT_CHUNK = 100

SYNC_PROJECT_TIMEZONES_LOCK_KEY = "conversation_ms:sync_project_timezones_active"
SYNC_PROJECT_TIMEZONES_LOCK_TTL_SECONDS = 7200

CLOSE_DAILY_LOCK_KEY = "conversation_ms:close_daily_active"

CLOSE_DAILY_PROJECT_LOCK_KEY_PREFIX = "conversation_ms:close_daily_project:"

RESOLUTION_IN_PROGRESS = "2"
TERMINAL_RESOLUTIONS = frozenset({"0", "1", "3", "4"})

CLOSE_PIPELINE_STAGES = ("classify", "topics", "billing", "datalake")


class ClosePipelineStageStatus:
    PENDING = "pending"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"

    CHOICES = (
        (PENDING, "Pending"),
        (DONE, "Done"),
        (SKIPPED, "Skipped"),
        (FAILED, "Failed"),
    )

    ALL = frozenset({PENDING, DONE, SKIPPED, FAILED})
    FINISHED = frozenset({DONE, SKIPPED})
    TERMINAL = frozenset({DONE, SKIPPED, FAILED})


class CloseDatalakeEventKind:
    CLASSIFICATION = "classification"
    TOPICS = "topics"

    CHOICES = (
        (CLASSIFICATION, "Classification"),
        (TOPICS, "Topics"),
    )

    ALL = frozenset({CLASSIFICATION, TOPICS})
