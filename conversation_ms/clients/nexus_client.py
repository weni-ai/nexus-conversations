import logging
from typing import Any

import requests
import sentry_sdk
from django.conf import settings

logger = logging.getLogger(__name__)


class NexusClient:
    DEFAULT_TIMEOUT = 30

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        configured_base_url = base_url if base_url is not None else getattr(settings, "NEXUS_API_BASE_URL", "")
        self.base_url = configured_base_url.rstrip("/")
        self.token = token if token is not None else getattr(settings, "NEXUS_API_TOKEN", None)
        self.timeout = timeout

    def _get_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _require_base_url(self) -> None:
        if not self.base_url:
            raise ValueError("NEXUS_API_BASE_URL is not configured")

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        log_prefix: str,
        context: dict[str, Any],
    ) -> Any:
        self._require_base_url()
        url = f"{self.base_url}{path}"

        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            sentry_sdk.capture_exception(exc)
            logger.error("[%s] Failed context=%s error=%s", log_prefix, context, exc, exc_info=True)
            raise

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
        self._require_base_url()
        url = f"{self.base_url}/api/agents/traces/"
        params = {"project_uuid": project_uuid, "log_id": log_id}

        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return self._normalize_traces_payload(response.json())
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                logger.info(
                    "[NexusClient.get_agent_traces] No traces found project_uuid=%s log_id=%s",
                    project_uuid,
                    log_id,
                )
                return []
            sentry_sdk.capture_exception(exc)
            logger.error(
                "[NexusClient.get_agent_traces] Failed project_uuid=%s log_id=%s error=%s",
                project_uuid,
                log_id,
                exc,
                exc_info=True,
            )
            raise
        except requests.RequestException as exc:
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
