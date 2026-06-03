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
    SQS_MESSAGES_QUEUE_URL=(str, ""),
    SQS_ROOMS_QUEUE_URL=(str, ""),
    SQS_CONVERSATION_DLQ_URL=(str, ""),
    SQS_CONVERSATION_REGION=(str, "us-east-1"),
    SQS_BILLING_QUEUE_URL=(str, ""),
    DYNAMODB_MESSAGE_TABLE=(str, ""),
    DYNAMODB_REGION=(str, "us-east-1"),
    USE_SENTRY=(bool, False),
    SENTRY_URL=(str, ""),
    ENVIRONMENT=(str, "development"),
    FILTER_SENTRY_EVENTS=(list, []),
    # Data Lake SDK (for CSAT/NPS)
    AGENT_UUID_CSAT=(str, ""),
    AGENT_UUID_NPS=(str, ""),
    JWT_PUBLIC_KEY=(str, ""),
    # EDA / AMQP broker
    EDA_BROKER_HOST=(str, "localhost"),
    EDA_BROKER_PORT=(int, 5672),
    EDA_BROKER_USER=(str, "guest"),
    EDA_BROKER_PASSWORD=(str, "guest"),
    EDA_VIRTUAL_HOST=(str, "/"),
    # OIDC / Keycloak
    OIDC_RP_SERVER_URL=(str, ""),
    OIDC_RP_REALM_NAME=(str, ""),
    OIDC_OP_JWKS_ENDPOINT=(str, ""),
    OIDC_RP_CLIENT_ID=(str, ""),
    OIDC_RP_CLIENT_SECRET=(str, ""),
    OIDC_OP_AUTHORIZATION_ENDPOINT=(str, ""),
    OIDC_OP_TOKEN_ENDPOINT=(str, ""),
    OIDC_OP_USER_ENDPOINT=(str, ""),
    OIDC_RP_SCOPES=(str, "openid email"),
    OIDC_RP_SIGN_ALGO=(str, "RS256"),
)
