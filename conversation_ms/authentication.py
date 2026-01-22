
from rest_framework import authentication, exceptions
from django.conf import settings
from drf_spectacular.extensions import OpenApiAuthenticationExtension


class InternalTokenAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION")
        if not auth_header:
            return None

        try:
            auth_type, token = auth_header.split()
        except ValueError:
            return None

        if auth_type.lower() != "bearer":
            return None

        return self._authenticate_credentials(token)

    def _authenticate_credentials(self, token):
        expected_token = getattr(settings, "INTERNAL_API_TOKEN", None)
        if not expected_token:
            raise exceptions.AuthenticationFailed("Internal API token not configured")

        if token != expected_token:
            raise exceptions.AuthenticationFailed("Invalid token")

        # Return (User, Auth) tuple.
        # Since we don't have a real user, we return None for user and the token for auth
        return (None, token)


class InternalTokenAuthenticationScheme(OpenApiAuthenticationExtension):
    target_class = "conversation_ms.authentication.InternalTokenAuthentication"
    name = "InternalTokenAuth"

    def get_security_definition(self, auto_schema):
        return {
            "type": "http",
            "scheme": "bearer",
            "description": "Internal API Token for microservice communication",
        }
