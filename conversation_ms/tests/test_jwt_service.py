from unittest.mock import patch

import jwt
import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from conversation_ms.api.internal.jwt_service import (
    MODULE_JWT_TTL_SECONDS,
    generate_project_jwt,
    read_project_uuid,
    resolve_project_uuid,
)


class TestGenerateProjectJwt:
    def test_raises_when_private_key_missing(self):
        with override_settings(JWT_SECRET_KEY=None):
            with pytest.raises(ImproperlyConfigured, match="JWT_SECRET_KEY"):
                generate_project_jwt("project-uuid")

    def test_encodes_project_uuid_with_ttl(self):
        with (
            override_settings(JWT_SECRET_KEY="private-pem"),
            patch("conversation_ms.api.internal.jwt_service.jwt.encode", return_value="signed-token") as mock_encode,
        ):
            token = generate_project_jwt("project-uuid")

        assert token == "signed-token"
        payload = mock_encode.call_args.args[0]
        assert payload["project_uuid"] == "project-uuid"
        assert (payload["exp"] - payload["iat"]).total_seconds() == MODULE_JWT_TTL_SECONDS
        assert mock_encode.call_args.kwargs["algorithm"] == "RS256"


class TestReadProjectUuid:
    def test_returns_none_without_public_key(self):
        with override_settings(JWT_PUBLIC_KEY=None):
            assert read_project_uuid("token") is None

    def test_returns_none_on_invalid_token(self):
        with (
            override_settings(JWT_PUBLIC_KEY=b"public-pem"),
            patch(
                "conversation_ms.api.internal.jwt_service.jwt.decode",
                side_effect=jwt.InvalidTokenError("bad"),
            ),
        ):
            assert read_project_uuid("token") is None

    def test_ignores_expiration(self):
        with (
            override_settings(JWT_PUBLIC_KEY=b"public-pem"),
            patch(
                "conversation_ms.api.internal.jwt_service.jwt.decode",
                return_value={"project_uuid": "recovered"},
            ) as mock_decode,
        ):
            assert read_project_uuid("queued.jwt.token") == "recovered"

        assert mock_decode.call_args.kwargs["options"]["verify_exp"] is False


class TestResolveProjectUuid:
    def test_returns_plain_uuid(self):
        assert resolve_project_uuid("76396786-80de-4dd1-b65a-31bf006435cc") == ("76396786-80de-4dd1-b65a-31bf006435cc")

    def test_recovers_uuid_from_queued_jwt(self):
        with patch(
            "conversation_ms.api.internal.jwt_service.read_project_uuid",
            return_value="recovered-uuid",
        ):
            assert resolve_project_uuid("header.payload.signature") == "recovered-uuid"

    def test_raises_when_queued_jwt_cannot_be_read(self):
        with patch("conversation_ms.api.internal.jwt_service.read_project_uuid", return_value=None):
            with pytest.raises(ValueError, match="Could not recover project_uuid"):
                resolve_project_uuid("header.payload.signature")

    def test_raises_when_empty(self):
        with pytest.raises(ValueError, match="project_uuid is required"):
            resolve_project_uuid("")

    def test_extra_dots_are_not_treated_as_jwt(self):
        assert resolve_project_uuid("a.b.c.d") == "a.b.c.d"
