from datetime import datetime, timedelta, timezone

import jwt
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

# Minted immediately before each outbound request, so a short lifetime is enough.
MODULE_JWT_TTL_SECONDS = 300
# Compact JWT form: header.payload.signature
_JWT_SEGMENT_SEPARATOR_COUNT = 2


def generate_project_jwt(project_uuid: str) -> str:
    """
    Mint an RS256 module-to-module token carrying ``project_uuid``.

    Callers must mint at request time: a token stored in a queue and used on a
    later retry may already be past its expiration.
    """
    private_key = getattr(settings, "JWT_PRIVATE_KEY", None)
    if not private_key:
        raise ImproperlyConfigured("JWT_PRIVATE_KEY is not configured")

    issued_at = datetime.now(timezone.utc)
    payload = {
        "project_uuid": str(project_uuid),
        "iat": issued_at,
        "exp": issued_at + timedelta(seconds=MODULE_JWT_TTL_SECONDS),
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


def read_project_uuid(token: str) -> str | None:
    """
    Return ``project_uuid`` from a signed token, ignoring expiration.

    Used to recover the project of work that was queued with a token instead of
    a plain UUID. The signature is still verified, so the value is trustworthy.
    """
    public_key = getattr(settings, "JWT_PUBLIC_KEY", None)
    if not public_key or not token:
        return None

    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"verify_aud": False, "verify_exp": False},
        )
    except jwt.PyJWTError:
        return None

    return payload.get("project_uuid")


def _looks_like_jwt(value: str) -> bool:
    """True for compact JWS (exactly three base64 segments)."""
    return value.count(".") == _JWT_SEGMENT_SEPARATOR_COUNT


def resolve_project_uuid(project_uuid_or_token: str) -> str:
    """
    Accept either a project UUID or a previously queued module JWT.

    Tasks already in the broker at deploy time still carry the inbound token
    as the first argument. Recover ``project_uuid`` from it so a fresh token
    can be minted instead of reusing the expired one.
    """
    if not project_uuid_or_token:
        raise ValueError("project_uuid is required")

    if _looks_like_jwt(project_uuid_or_token):
        recovered = read_project_uuid(project_uuid_or_token)
        if not recovered:
            raise ValueError("Could not recover project_uuid from queued token")
        return recovered

    return project_uuid_or_token
