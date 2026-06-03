import logging

from mozilla_django_oidc.auth import OIDCAuthenticationBackend

logger = logging.getLogger(__name__)


class WeniOIDCBackend(OIDCAuthenticationBackend):
    def verify_claims(self, claims):
        verified = super().verify_claims(claims)
        email = claims.get("email", "")
        return verified and bool(email)

    def get_username(self, claims):
        return claims.get("preferred_username", claims.get("email", ""))

    def filter_users_by_claims(self, claims):
        email = claims.get("email")
        if not email:
            return self.UserModel.objects.none()
        return self.UserModel.objects.filter(email=email)

    def create_user(self, claims):
        email = claims.get("email")
        username = self.get_username(claims)
        user, created = self.UserModel.objects.get_or_create(
            email=email,
            defaults={"username": username},
        )
        if created:
            user.set_unusable_password()
            user.save()
            logger.info("OIDC user created: %s", email)
        return user

    def update_user(self, user, claims):
        email = claims.get("email")
        username = self.get_username(claims)
        changed = False
        if email and user.email != email:
            user.email = email
            changed = True
        if username and user.username != username:
            user.username = username
            changed = True
        if changed:
            user.save()
            logger.info("OIDC user updated: %s", user.email)
        return user
