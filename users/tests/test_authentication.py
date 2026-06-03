from unittest.mock import patch

import pytest
from django.contrib.auth import get_user_model

from users.authentication import WeniOIDCBackend

User = get_user_model()

pytestmark = pytest.mark.django_db


@pytest.fixture
def backend():
    return WeniOIDCBackend()


class TestVerifyClaims:
    def test_valid_claims(self, backend):
        claims = {"email": "user@example.com", "sub": "abc-123"}
        assert backend.verify_claims(claims) is True

    def test_missing_email_returns_false(self, backend):
        claims = {"sub": "abc-123"}
        assert backend.verify_claims(claims) is False

    def test_empty_email_returns_false(self, backend):
        claims = {"email": "", "sub": "abc-123"}
        assert backend.verify_claims(claims) is False


class TestGetUsername:
    def test_preferred_username(self, backend):
        claims = {"preferred_username": "jdoe", "email": "jdoe@example.com"}
        assert backend.get_username(claims) == "jdoe"

    def test_fallback_to_email(self, backend):
        claims = {"email": "jdoe@example.com"}
        assert backend.get_username(claims) == "jdoe@example.com"


class TestFilterUsersByClaims:
    def test_returns_matching_user(self, backend):
        user = User.objects.create_user(email="match@example.com")
        result = backend.filter_users_by_claims({"email": "match@example.com"})
        assert list(result) == [user]

    def test_no_email_returns_empty(self, backend):
        result = backend.filter_users_by_claims({"sub": "123"})
        assert result.count() == 0


class TestCreateUser:
    def test_creates_new_user(self, backend):
        claims = {"email": "new@example.com", "preferred_username": "newuser"}
        user = backend.create_user(claims)
        assert user.email == "new@example.com"
        assert user.username == "newuser"
        assert not user.has_usable_password()

    def test_returns_existing_user(self, backend):
        existing = User.objects.create_user(email="existing@example.com", username="old")
        claims = {"email": "existing@example.com", "preferred_username": "newname"}
        user = backend.create_user(claims)
        assert user.pk == existing.pk


class TestUpdateUser:
    def test_updates_email_and_username(self, backend):
        user = User.objects.create_user(email="old@example.com", username="old")
        claims = {"email": "new@example.com", "preferred_username": "new"}
        updated = backend.update_user(user, claims)
        updated.refresh_from_db()
        assert updated.email == "new@example.com"
        assert updated.username == "new"

    def test_no_change_when_same_values(self, backend):
        user = User.objects.create_user(email="same@example.com", username="same")
        claims = {"email": "same@example.com", "preferred_username": "same"}
        with patch.object(User, "save") as mock_save:
            backend.update_user(user, claims)
            mock_save.assert_not_called()
