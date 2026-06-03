from unittest.mock import MagicMock, patch

import pytest
from django.core.cache import cache

from users.internals import CACHE_KEY, TOKEN_CACHE_TTL, InternalAuthentication


@pytest.fixture(autouse=True)
def _use_local_memory_cache(settings):
    settings.CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def auth_client():
    return InternalAuthentication()


class TestFetchToken:
    @patch("users.internals.requests.post")
    def test_returns_access_token(self, mock_post, auth_client):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "tok_123"},
        )
        mock_post.return_value.raise_for_status = MagicMock()
        token = auth_client._fetch_token()
        assert token == "tok_123"
        mock_post.assert_called_once()


class TestGetToken:
    @patch("users.internals.requests.post")
    def test_caches_token(self, mock_post, auth_client):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "cached_tok"},
        )
        mock_post.return_value.raise_for_status = MagicMock()

        first = auth_client._get_token()
        second = auth_client._get_token()
        assert first == second == "cached_tok"
        assert mock_post.call_count == 1

    @patch("users.internals.requests.post")
    def test_force_refresh_bypasses_cache(self, mock_post, auth_client):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "fresh_tok"},
        )
        mock_post.return_value.raise_for_status = MagicMock()

        cache.set(CACHE_KEY, "stale_tok", TOKEN_CACHE_TTL)
        token = auth_client._get_token(force_refresh=True)
        assert token == "fresh_tok"
        assert mock_post.call_count == 1


class TestHeaders:
    @patch("users.internals.requests.post")
    def test_returns_bearer_header(self, mock_post, auth_client):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "hdr_tok"},
        )
        mock_post.return_value.raise_for_status = MagicMock()
        assert auth_client.headers == {"Authorization": "Bearer hdr_tok"}


class TestMakeRequestWithRetry:
    @patch("users.internals.requests.post")
    def test_successful_request_no_retry(self, mock_token_post, auth_client):
        mock_token_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "tok"},
        )
        mock_token_post.return_value.raise_for_status = MagicMock()

        mock_method = MagicMock()
        mock_method.return_value = MagicMock(status_code=200)

        response = auth_client.make_request_with_retry(mock_method, "https://api.test/resource")
        assert response.status_code == 200
        assert mock_method.call_count == 1

    @patch("users.internals.requests.post")
    def test_retries_on_401(self, mock_token_post, auth_client):
        mock_token_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "retry_tok"},
        )
        mock_token_post.return_value.raise_for_status = MagicMock()

        first_response = MagicMock(status_code=401)
        second_response = MagicMock(status_code=200)
        mock_method = MagicMock(side_effect=[first_response, second_response])

        response = auth_client.make_request_with_retry(mock_method, "https://api.test/resource")
        assert response.status_code == 200
        assert mock_method.call_count == 2

    @patch("users.internals.requests.post")
    def test_retries_on_403(self, mock_token_post, auth_client):
        mock_token_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "retry_tok"},
        )
        mock_token_post.return_value.raise_for_status = MagicMock()

        first_response = MagicMock(status_code=403)
        second_response = MagicMock(status_code=200)
        mock_method = MagicMock(side_effect=[first_response, second_response])

        response = auth_client.make_request_with_retry(mock_method, "https://api.test/resource")
        assert response.status_code == 200
        assert mock_method.call_count == 2
