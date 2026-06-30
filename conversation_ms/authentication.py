from django.conf import settings
from drf_spectacular.extensions import OpenApiAuthenticationExtension
from rest_framework import authentication, exceptions


class ServiceUser:
    """
    A simple user class for service-to-service authentication.
    """

    def __init__(self, username):
        self.username = username
        self.is_authenticated = True

    def __str__(self):
        return self.username


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
        team_tokens = getattr(settings, "INTERNAL_API_TOKENS", {})
        if team_tokens:
            for team_name, team_token in team_tokens.items():
                if token == team_token:
                    return (ServiceUser(username=team_name), token)

        raise exceptions.AuthenticationFailed("Invalid token")


class InternalTokenAuthenticationScheme(OpenApiAuthenticationExtension):
    target_class = "conversation_ms.authentication.InternalTokenAuthentication"
    name = "InternalTokenAuth"

    def get_security_definition(self, auto_schema):
        return {
            "type": "http",
            "scheme": "bearer",
            "description": "Internal API Token for microservice communication",
        }


class BearerJWTAuthentication(authentication.BaseAuthentication):
    """
    Placeholder authentication for user JWT requests.
    Token validation is deferred to ProjectPermission via the external authorization API.
    """

    def authenticate(self, request):
        return None


class BearerJWTAuthenticationScheme(OpenApiAuthenticationExtension):
    target_class = "conversation_ms.authentication.BearerJWTAuthentication"
    name = "BearerJWT"

    def get_security_definition(self, auto_schema):
        return {
            "type": "http",
            "scheme": "bearer",
            "description": "User JWT forwarded to the project authorization API",
        }
