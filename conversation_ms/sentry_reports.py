"""Shared Sentry reporting for conversation creation (used by services and adapters)."""

from typing import Any, Dict, Optional

import sentry_sdk


def report_missing_required_sentry(
    reason: str,
    missing_fields: list,
    project_uuid: str,
    contact_urn: str,
    contact_name: str,
    channel_uuid: Optional[str],
    msg_created_at: Optional[str],
    event_metadata: Optional[Dict[str, Any]] = None,
    level: str = "error",
) -> None:
    """Send a complete report to Sentry when conversation creation is affected by missing info."""
    sentry_sdk.set_tag("project_uuid", project_uuid or "missing")
    sentry_sdk.set_tag("contact_urn", contact_urn or "missing")
    sentry_sdk.set_tag("event_type", (event_metadata or {}).get("event_type", "unknown"))
    sentry_sdk.set_tag("correlation_id", (event_metadata or {}).get("correlation_id", "unknown"))
    sentry_sdk.set_tag("reason", reason)

    context: Dict[str, Any] = {
        "project_uuid": project_uuid,
        "contact_urn": contact_urn,
        "contact_name": contact_name,
        "channel_uuid": channel_uuid,
        "msg_created_at": msg_created_at,
        "method": "ensure_conversation_exists",
        "reason": reason,
        "missing_fields": missing_fields,
    }
    if event_metadata:
        context["event_metadata"] = event_metadata
    sentry_sdk.set_context("conversation_creation_failed", context)
    sentry_sdk.capture_message(
        f"Conversation creation: {reason} (missing: {', '.join(missing_fields)})",
        level=level,
    )
