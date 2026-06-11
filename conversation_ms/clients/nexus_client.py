import logging
from typing import Any

import requests
import sentry_sdk
from django.conf import settings

from conversation_ms.internals import InternalAuthentication

logger = logging.getLogger(__name__)


class NexusClient:
    DEFAULT_TIMEOUT = 30

    def __init__(
        self,
        base_url: str | None = None,
        auth: InternalAuthentication | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        configured_base_url = base_url if base_url is not None else getattr(settings, "NEXUS_API_BASE_URL", "")
        self.base_url = configured_base_url.rstrip("/")
        self.auth = auth or InternalAuthentication()
        self.timeout = timeout

    def _require_base_url(self) -> None:
        if not self.base_url:
            raise ValueError("NEXUS_API_BASE_URL is not configured")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        log_prefix: str,
        context: dict[str, Any],
    ) -> requests.Response:
        self._require_base_url()
        url = f"{self.base_url}{path}"

        try:
            return self.auth.make_request_with_retry(
                method,
                url,
                params=params,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            sentry_sdk.capture_exception(exc)
            logger.error("[%s] Failed context=%s error=%s", log_prefix, context, exc, exc_info=True)
            raise

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        log_prefix: str,
        context: dict[str, Any],
    ) -> Any:
        response = self._request(
            "GET",
            path,
            params=params,
            log_prefix=log_prefix,
            context=context,
        )
        response.raise_for_status()
        return response.json()

    def get_project_customization(self, project_uuid: str) -> dict[str, Any]:
        """
        GET {NEXUS_API_BASE_URL}/api/{project_uuid}/customization/
        """
        payload = self._get_json(
            f"/api/{project_uuid}/customization/",
            log_prefix="NexusClient.get_project_customization",
            context={"project_uuid": project_uuid},
        )
        return payload

    def get_collaborative_agents(self, project_uuid: str) -> list[dict[str, Any]]:
        """
        GET {NEXUS_API_BASE_URL}/api/project/{project_uuid}/active-agents/config
        """
        payload = self._get_json(
            f"/api/project/{project_uuid}/active-agents/config",
            log_prefix="NexusClient.get_collaborative_agents",
            context={"project_uuid": project_uuid},
        )
        if isinstance(payload, list):
            return payload
        return []

    def get_agent_traces(self, project_uuid: str, log_id: str) -> list[dict[str, Any]]:
        """
        GET {NEXUS_API_BASE_URL}/api/agents/traces/?project_uuid={project_uuid}&log_id={log_id}
        """
        params = {"project_uuid": project_uuid, "log_id": log_id}
        try:
            response = self._request(
                "GET",
                "/api/agents/traces/",
                params=params,
                log_prefix="NexusClient.get_agent_traces",
                context={"project_uuid": project_uuid, "log_id": log_id},
            )
            if response.status_code == 404:
                logger.info(
                    "[NexusClient.get_agent_traces] No traces found project_uuid=%s log_id=%s",
                    project_uuid,
                    log_id,
                )
                return []
            response.raise_for_status()
            return self._normalize_traces_payload(response.json())
        except requests.HTTPError as exc:
            sentry_sdk.capture_exception(exc)
            logger.error(
                "[NexusClient.get_agent_traces] Failed project_uuid=%s log_id=%s error=%s",
                project_uuid,
                log_id,
                exc,
                exc_info=True,
            )
            raise

    @staticmethod
    def _normalize_traces_payload(payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [NexusClient._normalize_trace_item(item) for item in payload]
        if isinstance(payload, dict):
            for key in ("results", "traces", "data"):
                nested = payload.get(key)
                if isinstance(nested, list):
                    return [NexusClient._normalize_trace_item(item) for item in nested]
            return [NexusClient._normalize_trace_item(payload)]
        return []

    @staticmethod
    def _normalize_trace_item(item: Any) -> dict[str, Any]:
        if isinstance(item, dict) and "trace" in item and "config" not in item:
            return item
        if isinstance(item, dict):
            return {"trace": item}
        return {"trace": item}
