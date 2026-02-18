"""Tests for API authentication module."""

import pytest

from lhp.api.auth import DEV_USER, UserContext


pytestmark = pytest.mark.api


class TestUserContext:
    """Unit tests for the UserContext model."""

    def test_user_id_hash_is_16_char_hex(self):
        user = UserContext(email="a@b.com", username="alice", user_id="uid-123")
        h = user.user_id_hash
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_user_id_hash_is_deterministic(self):
        user = UserContext(email="a@b.com", username="alice", user_id="uid-123")
        assert user.user_id_hash == user.user_id_hash

    def test_different_user_ids_produce_different_hashes(self):
        u1 = UserContext(email="a@b.com", username="a", user_id="uid-1")
        u2 = UserContext(email="a@b.com", username="a", user_id="uid-2")
        assert u1.user_id_hash != u2.user_id_hash


class TestDevUser:
    """Tests for the DEV_USER constant."""

    def test_dev_user_email(self):
        assert DEV_USER.email == "dev@localhost"

    def test_dev_user_username(self):
        assert DEV_USER.username == "dev"

    def test_dev_user_user_id(self):
        assert DEV_USER.user_id == "dev-local"


class TestGetCurrentUserDevMode:
    """Tests for get_current_user via the /api/me endpoint in dev mode."""

    def test_me_returns_dev_user_in_dev_mode(self, client):
        resp = client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "dev@localhost"
        assert data["username"] == "dev"

    def test_me_returns_explicit_headers_even_in_dev_mode(self, client):
        headers = {
            "X-Forwarded-Email": "alice@corp.com",
            "X-Forwarded-User": "alice",
            "X-Forwarded-User-Id": "uid-alice",
        }
        resp = client.get("/api/me", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "alice@corp.com"
        assert data["username"] == "alice"
        assert data["user_id"] == "uid-alice"


class TestGetCurrentUserProdMode:
    """Tests for get_current_user via the /api/me endpoint in production mode."""

    def test_me_returns_401_without_headers_in_prod(self, prod_client):
        resp = prod_client.get("/api/me")
        assert resp.status_code == 401

    def test_me_returns_user_with_all_headers_in_prod(self, prod_client):
        headers = {
            "X-Forwarded-Email": "bob@corp.com",
            "X-Forwarded-User": "bob",
            "X-Forwarded-User-Id": "uid-bob",
        }
        resp = prod_client.get("/api/me", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "bob@corp.com"
        assert data["username"] == "bob"

    def test_me_returns_401_with_partial_headers_in_prod(self, prod_client):
        headers = {"X-Forwarded-Email": "bob@corp.com"}
        resp = prod_client.get("/api/me", headers=headers)
        assert resp.status_code == 401
