"""Structured logging helpers for archive pipeline observability."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def log_archive_event(event: str, **fields: Any) -> None:
    """Emit a structured archive log line with consistent field names."""
    parts = [f"{key}={value}" for key, value in sorted(fields.items()) if value is not None]
    logger.info("[Archive] %s %s", event, " ".join(parts))
