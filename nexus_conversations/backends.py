from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.backends import ModelBackend


class InternalTokenBackend(ModelBackend):
    """
    Authentication backend that authenticates against INTERNAL_API_TOKENS settings.
    Usage:
        Username: The key in INTERNAL_API_TOKENS (e.g., 'nexus')
        Password: The token value
    """

    def authenticate(self, request, username=None, password=None, **kwargs):
        internal_tokens = getattr(settings, "INTERNAL_API_TOKENS", {})

        if not internal_tokens or not isinstance(internal_tokens, dict):
            return None

        if username in internal_tokens and internal_tokens[username] == password:
            User = get_user_model()
            # Create or get a user for this internal service
            user, created = User.objects.get_or_create(username=username)

            # Ensure permissions
            if created or not user.is_superuser or not user.is_staff:
                user.is_staff = True
                user.is_superuser = True
                user.set_unusable_password()  # Prevent login via standard ModelBackend
                user.save()

            return user

        return None
