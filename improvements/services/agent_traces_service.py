from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests
import sentry_sdk
from django.conf import settings

from improvements.dependencies import get_improvements_dependencies

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = frozenset({429, 502, 503})


def _retry_delay_seconds(attempt: int, base: float) -> float:
    return (base * (2**attempt)) + random.uniform(0, base)


def _http_status(exc: requests.HTTPError) -> int | None:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def fetch_agent_traces(project_uuid: str, log_id: str) -> list[dict[str, Any]]:
    """
    Fetch agent traces with retry on transient Nexus failures.

    After exhausting retries (or on non-retryable errors), returns [] so a single
    flaky log_id does not abort the improvements build.
    """
    max_retries = int(getattr(settings, "IMPROVEMENTS_TRACES_MAX_RETRIES", 3))
    base_delay = float(getattr(settings, "IMPROVEMENTS_TRACES_RETRY_BASE_SECONDS", 1.0))
    project_data = get_improvements_dependencies().project_data
    last_exc: BaseException | None = None

    for attempt in range(max_retries + 1):
        try:
            return project_data.get_agent_traces(project_uuid, log_id)
        except requests.HTTPError as exc:
            last_exc = exc
            status = _http_status(exc)
            if status not in RETRYABLE_STATUS_CODES or attempt >= max_retries:
                break
            delay = _retry_delay_seconds(attempt, base_delay)
            logger.warning(
                "[fetch_agent_traces] Retryable HTTP error project_uuid=%s log_id=%s "
                "status=%s attempt=%s/%s delay_seconds=%.2f",
                project_uuid,
                log_id,
                status,
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            delay = _retry_delay_seconds(attempt, base_delay)
            logger.warning(
                "[fetch_agent_traces] Retryable network error project_uuid=%s log_id=%s "
                "attempt=%s/%s delay_seconds=%.2f error=%s",
                project_uuid,
                log_id,
                attempt + 1,
                max_retries,
                delay,
                exc,
            )
            time.sleep(delay)
        except Exception as exc:
            last_exc = exc
            logger.error(
                "[fetch_agent_traces] Unexpected error project_uuid=%s log_id=%s error=%s",
                project_uuid,
                log_id,
                exc,
                exc_info=True,
            )
            break

    if last_exc is not None:
        sentry_sdk.capture_exception(last_exc)
        logger.error(
            "[fetch_agent_traces] Degrading to empty traces project_uuid=%s log_id=%s error=%s",
            project_uuid,
            log_id,
            last_exc,
            exc_info=True,
        )
    return []
