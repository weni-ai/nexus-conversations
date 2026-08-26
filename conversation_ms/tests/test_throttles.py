from unittest.mock import MagicMock, patch

import pytest
from django.core.cache import InvalidCacheBackendError
from django_redis.exceptions import ConnectionInterrupted
from redis.exceptions import ConnectionError as RedisConnectionError
from rest_framework.throttling import SimpleRateThrottle

from conversation_ms.throttles import ConversationListRateThrottle


def test_allow_request_fail_open_on_redis_error():
    throttle = ConversationListRateThrottle()
    with patch.object(SimpleRateThrottle, "allow_request", side_effect=RedisConnectionError("down")):
        assert throttle.allow_request(MagicMock(), MagicMock()) is True


def test_allow_request_fail_open_on_django_redis_interrupt():
    throttle = ConversationListRateThrottle()
    with patch.object(SimpleRateThrottle, "allow_request", side_effect=ConnectionInterrupted("down")):
        assert throttle.allow_request(MagicMock(), MagicMock()) is True


def test_allow_request_fail_open_on_invalid_cache_backend():
    throttle = ConversationListRateThrottle()
    with patch.object(SimpleRateThrottle, "allow_request", side_effect=InvalidCacheBackendError("missing")):
        assert throttle.allow_request(MagicMock(), MagicMock()) is True


def test_allow_request_does_not_swallow_programming_errors():
    throttle = ConversationListRateThrottle()
    with patch.object(SimpleRateThrottle, "allow_request", side_effect=AttributeError("bug")):
        with pytest.raises(AttributeError):
            throttle.allow_request(MagicMock(), MagicMock())
