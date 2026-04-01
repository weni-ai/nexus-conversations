import jwt
from django.conf import settings
from rest_framework import authentication, exceptions


class JWTModuleAuthentication(authentication.BaseAuthentication):
    """
    RS256 JWT authentication for module-to-module communication.

    Validates the token using the RSA public key and returns the decoded
    payload as request.auth. No Django User is associated.
    """

    def authenticate(self, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION")
        if not auth_header:
            raise exceptions.AuthenticationFailed("Authorization header is missing")

        try:
            auth_type, token = auth_header.split()
        except ValueError:
            raise exceptions.AuthenticationFailed("Invalid authorization header format")

        if auth_type.lower() != "bearer":
            raise exceptions.AuthenticationFailed("Invalid authorization header format")

        public_key = getattr(settings, "JWT_PUBLIC_KEY", None)
        if not public_key:
            raise exceptions.AuthenticationFailed("JWT public key not configured")

        try:
            payload = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
        except jwt.ExpiredSignatureError:
            raise exceptions.AuthenticationFailed("Token has expired")
        except jwt.InvalidSignatureError:
            raise exceptions.AuthenticationFailed("Invalid token signature")
        except jwt.DecodeError:
            raise exceptions.AuthenticationFailed("Invalid token format")
        except jwt.InvalidTokenError:
            raise exceptions.AuthenticationFailed("Invalid token")
        except Exception:
            raise exceptions.AuthenticationFailed("Token validation failed")

        return (None, payload)

    def authenticate_header(self, request):
        return "Bearer"
