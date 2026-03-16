"""
Redis buffer for per-project conversation counts.
Uses INCR/DECR for high throughput; flush uses GETDEL (atomic read-and-delete).
"""

import logging
from typing import Generator

logger = logging.getLogger(__name__)

BUFFER_KEY_PREFIX = "project_count:buffer:"


def _get_redis():
    from django_redis import get_redis_connection

    return get_redis_connection("default")


def _buffer_key(project_uuid: str) -> str:
    return f"{BUFFER_KEY_PREFIX}{project_uuid}"


def increment(project_uuid: str) -> None:
    """Increment buffer count for project (conversation created)."""
    try:
        client = _get_redis()
        key = _buffer_key(project_uuid)
        client.incr(key)
    except Exception as e:
        logger.exception(
            "[ProjectCountBuffer] increment failed project_uuid=%s error=%s",
            project_uuid,
            e,
        )
        raise


def decrement(project_uuid: str) -> None:
    """Decrement buffer count for project (conversation deleted)."""
    try:
        client = _get_redis()
        key = _buffer_key(project_uuid)
        client.decr(key)
    except Exception as e:
        logger.exception(
            "[ProjectCountBuffer] decrement failed project_uuid=%s error=%s",
            project_uuid,
            e,
        )
        raise


def get(project_uuid: str) -> int:
    """Return current buffer value for project (for threshold check). Missing key => 0."""
    try:
        client = _get_redis()
        key = _buffer_key(project_uuid)
        raw = client.get(key)
        if raw is None:
            return 0
        return int(raw)
    except Exception as e:
        logger.exception(
            "[ProjectCountBuffer] get failed project_uuid=%s error=%s",
            project_uuid,
            e,
        )
        raise


def flush_key(project_uuid: str) -> int:
    """
    Atomically read and delete buffer for project. Returns the value that was in the key.
    Uses GETDEL (Redis 6.2+); no-op if key missing returns 0.
    """
    try:
        client = _get_redis()
        key = _buffer_key(project_uuid)
        if hasattr(client, "getdel"):
            raw = client.getdel(key)
        else:
            raw = _flush_key_lua(client, key)
        if raw is None:
            return 0
        return int(raw)
    except Exception as e:
        logger.exception(
            "[ProjectCountBuffer] flush_key failed project_uuid=%s error=%s",
            project_uuid,
            e,
        )
        raise


def _flush_key_lua(client, key: str):
    """Lua script: get value and delete key atomically. Returns value or None."""
    script = """
    local v = redis.call('GET', KEYS[1])
    if v == false then return nil end
    redis.call('DEL', KEYS[1])
    return v
    """
    try:
        return client.eval(script, 1, key)
    except Exception:
        # Some Redis clients use different eval signature
        return client.eval(script, 1, key)


def flush_all() -> Generator[tuple[str, int], None, None]:
    """
    SCAN for all buffer keys, GETDEL each, yield (project_uuid, delta).
    Each key is read-and-deleted atomically.
    """
    try:
        client = _get_redis()
        cursor = 0
        prefix = BUFFER_KEY_PREFIX
        prefix_len = len(prefix)
        while True:
            cursor, keys = client.scan(cursor=cursor, match=f"{prefix}*", count=100)
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode("utf-8")
                project_uuid = key[prefix_len:]
                delta = flush_key(project_uuid)
                if delta != 0:
                    yield project_uuid, delta
            if cursor == 0:
                break
    except Exception as e:
        logger.exception("[ProjectCountBuffer] flush_all failed error=%s", e)
        raise
