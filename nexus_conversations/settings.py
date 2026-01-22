"""
Django settings for nexus_conversations project.

Internal microservice for processing conversation messages from SQS FIFO queue.
"""

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

# Application definition

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django_celery_results",
    "django_celery_beat",
    "nexus_conversations.sentry",
    "conversation_ms.apps.ConversationMsConfig",  # Models for Conversation and ConversationMessages
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
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

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"
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

SQS_CONVERSATION_QUEUE_URL = env.str("SQS_CONVERSATION_QUEUE_URL", default="")
SQS_CONVERSATION_DLQ_URL = env.str("SQS_CONVERSATION_DLQ_URL", default="")
SQS_CONVERSATION_REGION = env.str("SQS_CONVERSATION_REGION", default="us-east-1")
SQS_CONVERSATION_ENABLED = env.bool("SQS_CONVERSATION_ENABLED", default=False)

# AWS General Configuration
AWS_ASSUME_ROLE_ARN = env.str("AWS_ASSUME_ROLE_ARN", default=None)

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
