from rest_framework.exceptions import AuthenticationFailed

from conversation_ms.api.internal.jwt_authenticators import JWTModuleAuthentication


class JWTModuleMixin:
    """
    Mixin to add JWT module authentication to API views.

    This mixin:
    - Enforces JWT authentication using JWTModuleAuthentication
    - Extracts project_uuid from the JWT payload
    - Makes it available as self.project_uuid

    Usage:
        class MyView(JWTModuleMixin, APIView):
            def post(self, request):
                project_uuid = self.project_uuid
                # ... rest of the view logic
    """

    authentication_classes = [JWTModuleAuthentication]

    def initial(self, request, *args, **kwargs):
        """
        Runs anything that needs to occur prior to calling the method handler.
        Extracts project_uuid from JWT payload.
        """
        super().initial(request, *args, **kwargs)

        if not request.auth:
            raise AuthenticationFailed("Authentication required")

        project_uuid = request.auth.get("project_uuid")
        if not project_uuid:
            raise AuthenticationFailed("project_uuid not found in token payload")

        self.project_uuid = project_uuid
