import environ

environ.Env.read_env(env_file=(environ.Path(__file__) - 2)(".env"))

env = environ.Env(
    SECRET_KEY=(str, "SK"),
    DEBUG=(bool, False),
    ALLOWED_HOSTS=(lambda v: [s.strip() for s in v.split(",")], list("*")),
    DEFAULT_DATABASE=(str, "sqlite:///db.sqlite3"),
    CELERY_BROKER_URL=(str, "redis://localhost:6379/0"),
    REDIS_URL=(str, "redis://localhost:6379/1"),
    REDIS_CHANNEL_URL=(str, "redis://localhost:6379/1"),
    SQS_CONVERSATION_QUEUE_URL=(str, ""),
    SQS_CONVERSATION_DLQ_URL=(str, ""),
    SQS_CONVERSATION_REGION=(str, "us-east-1"),
    DYNAMODB_MESSAGE_TABLE=(str, ""),
    DYNAMODB_REGION=(str, "us-east-1"),
    USE_SENTRY=(bool, False),
    SENTRY_URL=(str, ""),
    ENVIRONMENT=(str, "development"),
    FILTER_SENTRY_EVENTS=(list, []),
    # Data Lake SDK (for CSAT/NPS)
    AGENT_UUID_CSAT=(str, ""),
    AGENT_UUID_NPS=(str, ""),
)
