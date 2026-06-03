import logging

import requests
from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)

CACHE_KEY = "oidc:s2s:access_token"
TOKEN_CACHE_TTL = 250


class InternalAuthentication:
    """Client-credentials grant for service-to-service calls via Keycloak."""

    def _fetch_token(self):
        response = requests.post(
            settings.OIDC_OP_TOKEN_ENDPOINT,
            data={
                "grant_type": "client_credentials",
                "client_id": settings.OIDC_RP_CLIENT_ID,
                "client_secret": settings.OIDC_RP_CLIENT_SECRET,
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def _get_token(self, *, force_refresh=False):
        if not force_refresh:
            token = cache.get(CACHE_KEY)
            if token:
                return token
        token = self._fetch_token()
        cache.set(CACHE_KEY, token, TOKEN_CACHE_TTL)
        return token

    @property
    def headers(self):
        token = self._get_token()
        return {"Authorization": f"Bearer {token}"}

    def make_request_with_retry(self, method, url, **kwargs):
        """Execute an HTTP request; retry once with a fresh token on 401/403."""
        headers = kwargs.pop("headers", {})
        headers.update(self.headers)
        response = method(url, headers=headers, **kwargs)

        if response.status_code in (401, 403):
            logger.warning("S2S request got %s, retrying with fresh token", response.status_code)
            fresh_headers = {**headers, **{"Authorization": f"Bearer {self._get_token(force_refresh=True)}"}}
            response = method(url, headers=fresh_headers, **kwargs)

        return response
