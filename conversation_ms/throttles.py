import logging

from django.conf import settings
from rest_framework.throttling import SimpleRateThrottle

logger = logging.getLogger(__name__)


class ConversationListRateThrottle(SimpleRateThrottle):
    """
    Limit GET /projects/<project_uuid>/conversations/ per project.

    Internal calls share one service token, so throttling by user would cap
    the whole platform. A dump script against one project is keyed here.
    """

    scope = "conversation_list"

    def get_rate(self):
        rates = getattr(settings, "REST_FRAMEWORK", {}).get("DEFAULT_THROTTLE_RATES", {})
        return rates.get(self.scope)

    def allow_request(self, request, view):
        try:
            return super().allow_request(request, view)
        except Exception as exc:
            logger.warning(
                "[ConversationListRateThrottle] Cache unavailable, allowing request: %s",
                exc,
            )
            return True

    def get_cache_key(self, request, view):
        if getattr(view, "action", None) != "list":
            return None
        project_uuid = getattr(view, "kwargs", {}).get("project_uuid")
        if not project_uuid:
            return None
        return self.cache_format % {"scope": self.scope, "ident": str(project_uuid)}
