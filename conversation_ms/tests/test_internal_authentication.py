"""Tests for TokenCache and InternalAuthentication."""

import time
from unittest.mock import Mock, patch

import pytest
import requests

from conversation_ms.cache.token_cache import TokenCache
from conversation_ms.internals import InternalAuthentication, InternalAuthenticationTokenError


@pytest.fixture
def in_memory_cache():
    store: dict = {}

    def get(key, default=None):
        return store.get(key, default)

    def set_(key, value, timeout=None):
        store[key] = value

    def delete(key):
        store.pop(key, None)

    with patch("conversation_ms.cache.token_cache.cache") as mock_cache:
        mock_cache.get.side_effect = get
        mock_cache.set.side_effect = set_
        mock_cache.delete.side_effect = delete
        store.clear()
        yield store


class TestTokenCache:
    def test_get_returns_valid_token(self, in_memory_cache):
        token_cache = TokenCache(cache_key_prefix="test")
        in_memory_cache["test:main"] = {
            "token": "Bearer abc",
            "expires_at": time.time() + 3600,
        }

        assert token_cache.get("main") == "Bearer abc"

    def test_get_returns_none_and_deletes_when_expired(self, in_memory_cache):
        token_cache = TokenCache(cache_key_prefix="test")
        in_memory_cache["test:main"] = {
            "token": "Bearer abc",
            "expires_at": time.time() + 60,
        }

        assert token_cache.get("main") is None
        assert "test:main" not in in_memory_cache

    def test_get_or_generate_uses_cached_token(self, in_memory_cache):
        token_cache = TokenCache(cache_key_prefix="test")
        in_memory_cache["test:main"] = {
            "token": "Bearer cached",
            "expires_at": time.time() + 3600,
        }
        factory = Mock(return_value="Bearer new")

        result = token_cache.get_or_generate("main", factory)

        assert result == "Bearer cached"
        factory.assert_not_called()

    def test_get_or_generate_calls_factory_on_cache_miss(self, in_memory_cache):
        token_cache = TokenCache(cache_key_prefix="test")
        factory = Mock(return_value="Bearer new")

        result = token_cache.get_or_generate("main", factory)

        assert result == "Bearer new"
        factory.assert_called_once()
        assert in_memory_cache["test:main"]["token"] == "Bearer new"

    def test_get_or_generate_raises_when_factory_returns_empty(self, in_memory_cache):
        token_cache = TokenCache(cache_key_prefix="test")

        with pytest.raises(ValueError, match="Token factory returned empty token"):
            token_cache.get_or_generate("main", lambda: "")


@pytest.mark.django_db
class TestInternalAuthentication:
    @pytest.fixture
    def auth(self):
        return InternalAuthentication()

    @patch("conversation_ms.internals.requests.post")
    def test_fetch_token_from_keycloak_returns_bearer_token(self, mock_post, auth, settings):
        settings.OIDC_OP_TOKEN_ENDPOINT = "https://keycloak.example/token"
        settings.OIDC_RP_CLIENT_ID = "client-id"
        settings.OIDC_RP_CLIENT_SECRET = "client-secret"

        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "token-123"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        result = auth._fetch_token_from_keycloak()

        assert result == "Bearer token-123"
        mock_post.assert_called_once_with(
            url="https://keycloak.example/token",
            data={
                "client_id": "client-id",
                "client_secret": "client-secret",
                "grant_type": "client_credentials",
            },
            timeout=30,
        )

    @patch("conversation_ms.internals.requests.post")
    def test_fetch_token_raises_when_access_token_missing(self, mock_post, auth, settings):
        settings.OIDC_OP_TOKEN_ENDPOINT = "https://keycloak.example/token"
        settings.OIDC_RP_CLIENT_ID = "client-id"
        settings.OIDC_RP_CLIENT_SECRET = "client-secret"

        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with pytest.raises(InternalAuthenticationTokenError, match="Access token not found"):
            auth._fetch_token_from_keycloak()

    @patch("conversation_ms.internals.requests.post")
    def test_fetch_token_raises_on_request_failure(self, mock_post, auth, settings):
        settings.OIDC_OP_TOKEN_ENDPOINT = "https://keycloak.example/token"
        settings.OIDC_RP_CLIENT_ID = "client-id"
        settings.OIDC_RP_CLIENT_SECRET = "client-secret"

        mock_post.side_effect = requests.exceptions.ConnectionError("connection failed")

        with pytest.raises(InternalAuthenticationTokenError, match="Failed to fetch token"):
            auth._fetch_token_from_keycloak()

    @patch.object(InternalAuthentication, "_fetch_token_from_keycloak", return_value="Bearer fresh")
    def test_headers_uses_cached_token(self, mock_fetch, auth, in_memory_cache):
        in_memory_cache["keycloak_internal:main"] = {
            "token": "Bearer cached",
            "expires_at": time.time() + 3600,
        }

        headers = auth.headers

        assert headers["Authorization"] == "Bearer cached"
        assert headers["Content-Type"] == "application/json; charset: utf-8"
        mock_fetch.assert_not_called()

    @patch("conversation_ms.internals.requests.request")
    @patch.object(InternalAuthentication, "_fetch_token_from_keycloak", return_value="Bearer fresh")
    def test_make_request_with_retry_invalidates_cache_on_401(self, mock_fetch, mock_request, auth, in_memory_cache):
        in_memory_cache["keycloak_internal:main"] = {
            "token": "Bearer stale",
            "expires_at": time.time() + 3600,
        }

        unauthorized = Mock(status_code=401)
        success = Mock(status_code=200)
        mock_request.side_effect = [unauthorized, success]

        response = auth.make_request_with_retry("GET", "https://api.example/resource")

        assert response.status_code == 200
        assert mock_request.call_count == 2
        assert mock_fetch.call_count == 1
        assert in_memory_cache["keycloak_internal:main"]["token"] == "Bearer fresh"

    @patch("conversation_ms.internals.requests.request")
    @patch.object(InternalAuthentication, "_fetch_token_from_keycloak", return_value="Bearer fresh")
    def test_make_request_with_retry_invalidates_cache_on_403(self, mock_fetch, mock_request, auth, in_memory_cache):
        in_memory_cache["keycloak_internal:main"] = {
            "token": "Bearer stale",
            "expires_at": time.time() + 3600,
        }

        forbidden = Mock(status_code=403)
        success = Mock(status_code=200)
        mock_request.side_effect = [forbidden, success]

        response = auth.make_request_with_retry("GET", "https://api.example/resource")

        assert response.status_code == 200
        assert mock_request.call_count == 2
        assert mock_fetch.call_count == 1
