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
        authorization: str | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        self._require_base_url()
        url = f"{self.base_url}{path}"

        try:
            if authorization is not None:
                headers = dict(kwargs.pop("headers", {}) or {})
                headers["Authorization"] = authorization
                headers.setdefault("Content-Type", "application/json; charset: utf-8")
                return requests.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    **kwargs,
                )
            return self.auth.make_request_with_retry(
                method,
                url,
                params=params,
                timeout=self.timeout,
                **kwargs,
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

    def _post_json(
        self,
        path: str,
        *,
        payload: dict[str, Any],
        log_prefix: str,
        context: dict[str, Any],
        authorization: str | None = None,
    ) -> Any:
        response = self._request(
            "POST",
            path,
            log_prefix=log_prefix,
            context=context,
            authorization=authorization,
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def open_support_ticket(
        self,
        project_uuid: str,
        payload: dict[str, Any],
        *,
        authorization: str,
    ) -> Any:
        """
        POST {NEXUS_API_BASE_URL}/api/{project_uuid}/improvements/open-support-ticket/

        Forwards the caller ``Authorization`` header to Nexus. Does not use the internal
        Keycloak module token.
        """
        if not str(authorization or "").strip():
            raise ValueError("Authorization header is required")

        return self._post_json(
            f"/api/{project_uuid}/improvements/open-support-ticket/",
            payload=payload,
            log_prefix="NexusClient.open_support_ticket",
            context={"project_uuid": project_uuid},
            authorization=authorization,
        )

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

    def get_ai_resolution_criteria(self, project_uuid: str) -> dict[str, Any]:
        """
        GET {NEXUS_API_BASE_URL}/api/{project_uuid}/ai-resolution-criteria/
        """
        payload = self._get_json(
            f"/api/{project_uuid}/ai-resolution-criteria/",
            log_prefix="NexusClient.get_ai_resolution_criteria",
            context={"project_uuid": project_uuid},
        )
        return payload if isinstance(payload, dict) else {}

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

    def get_knowledge_base_chunks(self, project_uuid: str) -> list[dict[str, Any]]:
        """
        GET {NEXUS_API_BASE_URL}/api/{project_uuid}/knowledge-base/chunks
        Paginates with cursor until next_cursor is absent or max chunk limit is reached.
        """
        max_chunks = getattr(settings, "IMPROVEMENTS_KNOWLEDGE_BASE_MAX_CHUNKS", 0)
        accumulated: list[dict[str, Any]] = []
        cursor: str | None = None
        page = 0
        total_count: int | None = None

        while True:
            page += 1
            params = {"cursor": cursor} if cursor else None
            try:
                response = self._request(
                    "GET",
                    f"/api/{project_uuid}/knowledge-base/chunks",
                    params=params,
                    log_prefix="NexusClient.get_knowledge_base_chunks",
                    context={"project_uuid": project_uuid, "page": page, "cursor": cursor},
                )
                if response.status_code == 404:
                    logger.info(
                        "[NexusClient.get_knowledge_base_chunks] No knowledge base found project_uuid=%s",
                        project_uuid,
                    )
                    return []
                response.raise_for_status()
            except requests.HTTPError as exc:
                sentry_sdk.capture_exception(exc)
                logger.error(
                    "[NexusClient.get_knowledge_base_chunks] Failed project_uuid=%s page=%s error=%s",
                    project_uuid,
                    page,
                    exc,
                    exc_info=True,
                )
                raise

            payload = response.json()
            if not isinstance(payload, dict):
                break

            if total_count is None:
                api_count = payload.get("count")
                if isinstance(api_count, int):
                    total_count = api_count

            results = payload.get("results")
            if not isinstance(results, list):
                results = []

            for item in results:
                accumulated.append(self._normalize_knowledge_base_chunk(item))
                if max_chunks > 0 and len(accumulated) >= max_chunks:
                    if total_count is not None and total_count > max_chunks:
                        logger.warning(
                            "[NexusClient.get_knowledge_base_chunks] Truncated knowledge base "
                            "project_uuid=%s api_count=%s included=%s max_chunks=%s",
                            project_uuid,
                            total_count,
                            max_chunks,
                            max_chunks,
                        )
                    return accumulated[:max_chunks]

            logger.info(
                "[NexusClient.get_knowledge_base_chunks] Fetched page project_uuid=%s page=%s "
                "accumulated=%s api_count=%s",
                project_uuid,
                page,
                len(accumulated),
                total_count,
            )

            next_cursor = payload.get("next_cursor")
            if not next_cursor:
                break
            cursor = str(next_cursor)

        return accumulated

    @staticmethod
    def _normalize_knowledge_base_chunk(item: Any) -> dict[str, Any]:
        if not isinstance(item, dict):
            return {"chunk_id": "", "content": str(item)}
        return {
            "chunk_id": item.get("id", ""),
            "content": item.get("text", ""),
            "filename": item.get("filename"),
            "file_uuid": item.get("file_uuid"),
        }
