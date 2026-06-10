"""
Django settings for nexus_conversations project.

Internal microservice for processing conversation messages from SQS FIFO queue.
"""

import base64
import os
import sys
from pathlib import Path

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
    "drf_spectacular",
    "django_filters",
    "nexus_conversations.sentry",
    "conversation_ms.apps.ConversationMsConfig",  # Models for Conversation and ConversationMessages
    "improvements.apps.ImprovementsConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
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

# Close daily Celery task: distributed lock (TTL should exceed typical run; tune via metrics)
CLOSE_DAILY_LOCK_ENABLED = env.bool("CLOSE_DAILY_LOCK_ENABLED", default=not TESTING)
CLOSE_DAILY_LOCK_TTL_SECONDS = env.int("CLOSE_DAILY_LOCK_TTL_SECONDS", default=7200)
# Max IN_PROGRESS conversations per project per normal run (0 = unlimited). Reduces long single-task runs.
CLOSE_DAILY_MAX_CONVERSATIONS_PER_PROJECT = env.int("CLOSE_DAILY_MAX_CONVERSATIONS_PER_PROJECT", default=0)

# Per-project sub-task limits (fan-out architecture)
CLOSE_DAILY_PROJECT_SOFT_TIME_LIMIT = env.int("CLOSE_DAILY_PROJECT_SOFT_TIME_LIMIT", default=1800)
CLOSE_DAILY_PROJECT_TIME_LIMIT = env.int("CLOSE_DAILY_PROJECT_TIME_LIMIT", default=2100)
CLOSE_DAILY_PROJECT_LOCK_TTL_SECONDS = env.int("CLOSE_DAILY_PROJECT_LOCK_TTL_SECONDS", default=2400)

# Parallel classification within each batch (ThreadPoolExecutor)
CLOSE_DAILY_CLASSIFICATION_THREADS = env.int("CLOSE_DAILY_CLASSIFICATION_THREADS", default=5)

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

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
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")

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
CLASSIFICATION_LAMBDA_NAME = env.str("CLASSIFICATION_LAMBDA_NAME", default="nexus-classification-prod")
GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = env.str("GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN", default=None)

# Nexus API (project customization)
NEXUS_API_BASE_URL = env.str("NEXUS_API_BASE_URL", default="https://nexus.stg.cloud.weni.ai")
NEXUS_API_TOKEN = env.str("NEXUS_API_TOKEN", default="")

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
CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = env.int("CONVERSATIONS_IMPROVEMENTS_TRHESHOLD", default=0)

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

AWS_REGION = env.str("AWS_REGION", default="sa-east-1")
LAMBDA_AWS_REGION = env.str("LAMBDA_AWS_REGION", default="us-east-1")

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
