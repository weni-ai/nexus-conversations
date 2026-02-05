import logging

import requests
import sentry_sdk
from django.conf import settings

logger = logging.getLogger(__name__)


class ProjectClient:
    def __init__(self):
        self.base_url = settings.PROJECTS_API_BASE_URL
        self.token = getattr(settings, "PROJECTS_API_TOKEN", None)
        self.page_size = getattr(settings, "PROJECTS_PAGE_SIZE", 100)

    def _get_headers(self) -> dict:
        """Get authorization headers."""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def get_projects_paginated(self, page: int = 1, page_size: int = None) -> dict:
        """
        Fetch projects from external API with pagination.

        Args:
            page: Page number to fetch (1-indexed)
            page_size: Number of projects per page. Defaults to settings.PROJECTS_PAGE_SIZE

        Returns:
            Response JSON with structure:
            {
                "results": [{"uuid": "...", "timezone": "..."}, ...],
                "count": total_count,
                "next": "url_to_next_page" or null,
                "previous": "url_to_previous_page" or null
            }

        Raises:
            requests.RequestException: If the request fails
        """
        if page_size is None:
            page_size = self.page_size

        url = f"{self.base_url}/v2/internals/connect/projects"
        params = {"page": page, "page_size": page_size}

        try:
            response = requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            sentry_sdk.capture_exception(e)
            logger.error(
                f"[ProjectClient] Error fetching projects page {page}",
                extra={"page": page, "page_size": page_size, "error": str(e)},
                exc_info=True,
            )
            raise
