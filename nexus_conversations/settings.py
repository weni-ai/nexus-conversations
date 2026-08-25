"""
Django settings for nexus_conversations project.

Internal microservice for processing conversation messages from SQS FIFO queue.
"""

import base64
import os
import sys
from pathlib import Path

from corsheaders.defaults import default_headers

from .environment import env

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Treat any pytest run or manage.py test as testing
TESTING = (
    any("pytest" in arg for arg in sys.argv)
    or any(arg == "test" for arg in sys.argv)
    or os.environ.get("PYTEST_CURRENT_TEST") is not None
)

# Quick-start development settings
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env.str("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env.bool("DEBUG")

ALLOWED_HOSTS = env.list("ALLOWED_HOSTS")
CSRF_TRUSTED_ORIGINS = env.list("CSRF_TRUSTED_ORIGINS", default=[])

CORS_ALLOW_ALL_ORIGINS = env.bool("CORS_ORIGIN_ALLOW_ALL", default=DEBUG)

if not CORS_ALLOW_ALL_ORIGINS:
    CORS_ALLOWED_ORIGINS = env.list("CORS_ALLOWED_ORIGINS", default=[])

CORS_ALLOW_CREDENTIALS = env.bool("CORS_ALLOW_CREDENTIALS", default=False)
CORS_ALLOW_HEADERS = list(default_headers)

# Internal API Tokens
# Team-based tokens support (JSON dict: {"TeamName": "Token"})
INTERNAL_API_TOKENS = env.json("INTERNAL_API_TOKENS", default={})

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_celery_results",
    "django_celery_beat",
    "rest_framework",
    "corsheaders",
    "drf_spectacular",
    "django_filters",
    "nexus_conversations.sentry",
    "conversation_ms.apps.ConversationMsConfig",  # Models for Conversation and ConversationMessages
    "improvements.apps.ImprovementsConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "nexus_conversations.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "nexus_conversations.wsgi.application"
ASGI_APPLICATION = "nexus_conversations.asgi.application"

# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {"default": env.db(var="DEFAULT_DATABASE", default="sqlite:///db.sqlite3")}

# Allow CI or local env to OPT-IN to sqlite by setting USE_SQLITE_FOR_TESTS=true
USE_SQLITE_FOR_TESTS = env.bool("USE_SQLITE_FOR_TESTS", default=True)
if TESTING and USE_SQLITE_FOR_TESTS:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": str(BASE_DIR / "test.sqlite3"),
        }
    }

# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

# Timezone Configuration
FALLBACK_TIMEZONE = env.str("FALLBACK_TIMEZONE", default="America/Sao_Paulo")

# Close daily Celery task: distributed lock
CLOSE_DAILY_LOCK_ENABLED = env.bool("CLOSE_DAILY_LOCK_ENABLED", default=not TESTING)
CLOSE_DAILY_LOCK_TTL_SECONDS = env.int("CLOSE_DAILY_LOCK_TTL_SECONDS", default=3600)
# Max IN_PROGRESS conversations per project per normal run (0 = unlimited)
CLOSE_DAILY_MAX_CONVERSATIONS_PER_PROJECT = env.int("CLOSE_DAILY_MAX_CONVERSATIONS_PER_PROJECT", default=0)

# Per-project selector limits (claim + enqueue)
CLOSE_DAILY_PROJECT_SOFT_TIME_LIMIT = env.int("CLOSE_DAILY_PROJECT_SOFT_TIME_LIMIT", default=300)
CLOSE_DAILY_PROJECT_TIME_LIMIT = env.int("CLOSE_DAILY_PROJECT_TIME_LIMIT", default=600)
CLOSE_DAILY_PROJECT_LOCK_TTL_SECONDS = env.int("CLOSE_DAILY_PROJECT_LOCK_TTL_SECONDS", default=900)

# Close-pipeline stage workers and drain
CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS = env.int("CLOSE_PIPELINE_PENDING_HEARTBEAT_SECONDS", default=600)
CLOSE_PIPELINE_CLASSIFY_MAX_RETRIES = env.int("CLOSE_PIPELINE_CLASSIFY_MAX_RETRIES", default=3)
CLOSE_PIPELINE_TOPICS_MAX_RETRIES = env.int("CLOSE_PIPELINE_TOPICS_MAX_RETRIES", default=3)
CLOSE_PIPELINE_BILLING_MAX_RETRIES = env.int("CLOSE_PIPELINE_BILLING_MAX_RETRIES", default=5)
CLOSE_PIPELINE_DATALAKE_MAX_RETRIES = env.int("CLOSE_PIPELINE_DATALAKE_MAX_RETRIES", default=5)
CLOSE_PIPELINE_STALE_PENDING_SECONDS = env.int("CLOSE_PIPELINE_STALE_PENDING_SECONDS", default=1800)
CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS = env.int("CLOSE_PIPELINE_MAX_DRAIN_RECLAIMS", default=5)
CLOSE_PIPELINE_DRAIN_BATCH_SIZE = env.int("CLOSE_PIPELINE_DRAIN_BATCH_SIZE", default=100)
CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE = env.bool("CLOSE_PIPELINE_BILLING_OUTAGE_PAUSE", default=False)

AI_RESOLUTION_CRITERIA_CACHE_TTL_SECONDS = env.int("AI_RESOLUTION_CRITERIA_CACHE_TTL_SECONDS", default=3600)

CELERY_TASK_ROUTES = {
    # Separate queues for backlog visibility; consumed by the same conversations-celery pod (-Q).
    "conversation_ms.tasks.close_pipeline_classify_task": {"queue": "close_lambda"},
    "conversation_ms.tasks.close_pipeline_topics_task": {"queue": "close_lambda"},
    "conversation_ms.tasks.close_pipeline_billing_task": {"queue": "close_billing"},
    "conversation_ms.tasks.close_pipeline_datalake_task": {"queue": "close_datalake"},
}

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")

STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

AUTHENTICATION_BACKENDS = [
    "nexus_conversations.backends.InternalTokenBackend",
    "django.contrib.auth.backends.ModelBackend",
]

# Django REST Framework
REST_FRAMEWORK = {
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 20,
    "DEFAULT_FILTER_BACKENDS": ["django_filters.rest_framework.DjangoFilterBackend"],
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": [],
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

SPECTACULAR_SETTINGS = {
    "TITLE": "Nexus Conversations Microservice API",
    "DESCRIPTION": "Internal microservice for processing conversation messages from SQS FIFO queue.",
    "VERSION": "1.0.0",
    "SERVE_INCLUDE_SCHEMA": False,
    "COMPONENT_SPLIT_REQUEST": True,
}

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Redis Config
REDIS_URL = env.str("REDIS_URL", default=env.str("CELERY_BROKER_URL", default="redis://localhost:6379/1"))

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    }
}

# Celery config
CELERY_RESULT_BACKEND = "django-db"
CELERY_BROKER_URL = env.str("CELERY_BROKER_URL", default="redis://localhost:6379/0")
CELERY_ACCEPT_CONTENT = ["application/json"]
CELERY_RESULT_SERIALIZER = "json"
CELERY_TASK_SERIALIZER = "json"
CELERY_BEAT_SCHEDULER = "redbeat.RedBeatScheduler"
CELERY_REDBEAT_REDIS_URL = env.str("CELERY_REDBEAT_REDIS_URL", default=CELERY_BROKER_URL)
CELERY_REDBEAT_KEY_PREFIX = env.str("CELERY_REDBEAT_KEY_PREFIX", default="redbeat:")

# SQS Configuration for Conversation MS
SQS_MESSAGES_QUEUE_URL = env.str("SQS_MESSAGES_QUEUE_URL", default="")
SQS_ROOMS_QUEUE_URL = env.str("SQS_ROOMS_QUEUE_URL", default="")
SQS_CONVERSATION_QUEUE_URL = env.str("SQS_CONVERSATION_QUEUE_URL", default="")
SQS_CONVERSATION_DLQ_URL = env.str("SQS_CONVERSATION_DLQ_URL", default="")
SQS_CONVERSATION_REGION = env.str("SQS_CONVERSATION_REGION", default="us-east-1")
SQS_CONVERSATION_ENABLED = env.bool("SQS_CONVERSATION_ENABLED", default=False)

# Outbound SQS (FIFO) for billing
SQS_BILLING_QUEUE_URL = env.str("SQS_BILLING_QUEUE_URL", default="")

# AWS General Configuration
AWS_ASSUME_ROLE_ARN = env.str("AWS_ASSUME_ROLE_ARN", default=None)
CONVERSATION_TOPIC_CLASSIFIER_NAME = env.str("CONVERSATION_TOPIC_CLASSIFIER_NAME", default=None)
CONVERSATION_RESOLUTION_NAME = env.str("CONVERSATION_RESOLUTION_NAME", default=None)
CONVERSATION_RESOLUTION_V2_NAME = env.str("CONVERSATION_RESOLUTION_V2_NAME", default=None)
CONVERSATION_RESOLUTION_LEGACY_PROJECTS = env.list("CONVERSATION_RESOLUTION_LEGACY_PROJECTS", default=[])
CLASSIFICATION_LAMBDA_NAME = env.str("CLASSIFICATION_LAMBDA_NAME", default="nexus-classification-prod")
GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = env.str("GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN", default=None)

# Nexus API (project customization)
NEXUS_API_BASE_URL = env.str("NEXUS_API_BASE_URL", default="https://nexus.stg.cloud.weni.ai")

# Improvements JSON output
IMPROVEMENTS_S3_BUCKET = env.str("IMPROVEMENTS_S3_BUCKET", default="")
IMPROVEMENTS_S3_PREFIX = env.str("IMPROVEMENTS_S3_PREFIX", default="improvements")
IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION = env.int("IMPROVEMENTS_S3_PRESIGNED_URL_EXPIRATION", default=3600)
IMPROVEMENTS_ANALYSIS_LAMBDA_NAME = env.str(
    "IMPROVEMENTS_ANALYSIS_LAMBDA_NAME",
    default="conversations_improvements_analisys",
)
IMPROVEMENTS_SAMPLING_MODE = env.str("IMPROVEMENTS_SAMPLING_MODE", default="stratified_by_time_window")
IMPROVEMENTS_COMPLETION_WINDOW = env.str("IMPROVEMENTS_COMPLETION_WINDOW", default="24h")
IMPROVEMENTS_CONVERSATION_BATCH_SIZE = env.int("IMPROVEMENTS_CONVERSATION_BATCH_SIZE", default=50)
IMPROVEMENTS_BATCH_CHECK_INTERVAL_SECONDS = env.int("IMPROVEMENTS_BATCH_CHECK_INTERVAL_SECONDS", default=300)
IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS = env.int("IMPROVEMENTS_BATCH_CHECK_TIMEOUT_SECONDS", default=86400)
IMPROVEMENTS_RUN_METADATA_TTL_SECONDS = env.int("IMPROVEMENTS_RUN_METADATA_TTL_SECONDS", default=604800)
IMPROVEMENTS_KNOWLEDGE_BASE_FETCH_ENABLED = env.bool("IMPROVEMENTS_KNOWLEDGE_BASE_FETCH_ENABLED", default=True)
IMPROVEMENTS_KNOWLEDGE_BASE_MAX_CHUNKS = env.int("IMPROVEMENTS_KNOWLEDGE_BASE_MAX_CHUNKS", default=0)
IMPROVEMENTS_TRACES_MAX_WORKERS = env.int("IMPROVEMENTS_TRACES_MAX_WORKERS", default=8)
IMPROVEMENTS_TRACES_MAX_RETRIES = env.int("IMPROVEMENTS_TRACES_MAX_RETRIES", default=3)
IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS = env.float("IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS", default=1.0)
IMPROVEMENTS_BUILD_SOFT_TIME_LIMIT_SECONDS = env.int("IMPROVEMENTS_BUILD_SOFT_TIME_LIMIT_SECONDS", default=1500)
IMPROVEMENTS_BUILD_TIME_LIMIT_SECONDS = env.int("IMPROVEMENTS_BUILD_TIME_LIMIT_SECONDS", default=1800)
IMPROVEMENTS_BUILDING_TIMEOUT_SECONDS = env.int("IMPROVEMENTS_BUILDING_TIMEOUT_SECONDS", default=2700)
CONVERSATIONS_IMPROVEMENTS_THRESHOLD = env.int("CONVERSATIONS_IMPROVEMENTS_THRESHOLD", default=0)

# DynamoDB Configuration
DYNAMODB_REGION = env.str("DYNAMODB_REGION", default="us-east-1")
DYNAMODB_MESSAGE_TABLE = env.str("DYNAMODB_MESSAGE_TABLE", default="NexusMessages")

# Sentry config
USE_SENTRY = env.bool("USE_SENTRY", default=False)
SENTRY_URL = env.str("SENTRY_URL", default="")
ENVIRONMENT = env.str("ENVIRONMENT", default="development")
FILTER_SENTRY_EVENTS = env.list("FILTER_SENTRY_EVENTS", default=[])

# Data Lake SDK (for CSAT/NPS)
AGENT_UUID_CSAT = env.str("AGENT_UUID_CSAT", default="")
AGENT_UUID_NPS = env.str("AGENT_UUID_NPS", default="")

# Logging configuration
LOG_LEVEL = env.str("LOG_LEVEL", default="INFO")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": LOG_LEVEL,
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "conversation_ms": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
    },
}

# Client variables
BILLING_BASE_URL = env.str("BILLING_BASE_URL", default="")
BILLING_TOKEN = env.str("BILLING_TOKEN", default="")

# Projects API Configuration
PROJECTS_API_BASE_URL = env.str("PROJECTS_API_BASE_URL", default="")
PROJECTS_API_TOKEN = env.str("PROJECTS_API_TOKEN", default="")
PROJECTS_PAGE_SIZE = env.int("PROJECTS_PAGE_SIZE", default=100)
PROJECT_AUTH_API_TIMEOUT_SECONDS = env.int("PROJECT_AUTH_API_TIMEOUT_SECONDS", default=5)

# Keycloak OIDC (client_credentials for internal service-to-service calls)
OIDC_OP_TOKEN_ENDPOINT = env.str("OIDC_OP_TOKEN_ENDPOINT", default="")
OIDC_RP_CLIENT_ID = env.str("OIDC_RP_CLIENT_ID", default="")
OIDC_RP_CLIENT_SECRET = env.str("OIDC_RP_CLIENT_SECRET", default="")

AWS_REGION = env.str("AWS_REGION", default="sa-east-1")
LAMBDA_AWS_REGION = env.str("LAMBDA_AWS_REGION", default="us-east-1")
IMPROVEMENTS_LAMBDA_AWS_REGION = env.str("IMPROVEMENTS_LAMBDA_AWS_REGION", default="sa-east-1")
IMPROVEMENTS_LAMBDA_READ_TIMEOUT_SECONDS = env.int(
    "IMPROVEMENTS_LAMBDA_READ_TIMEOUT_SECONDS",
    default=300,
)

DATALAKE_FEATURE_FLAG = env.list("DATALAKE_FEATURE_FLAG", default=[])


JWT_PUBLIC_KEY_ENV = env.str("JWT_PUBLIC_KEY", default="")
if JWT_PUBLIC_KEY_ENV:
    JWT_PUBLIC_KEY = base64.b64decode(JWT_PUBLIC_KEY_ENV)
else:
    JWT_PUBLIC_KEY_PATH = BASE_DIR / "jwt_keys" / "public_key.pem"
    try:
        with open(JWT_PUBLIC_KEY_PATH, "rb") as f:
            JWT_PUBLIC_KEY = f.read()
    except FileNotFoundError:
        JWT_PUBLIC_KEY = None
