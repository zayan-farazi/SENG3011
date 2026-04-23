import os

import pytest
import requests

from constants import AUTH_PATH, STATUS_BAD_REQUEST, STATUS_OK, STATUS_UNAUTHORIZED


BASE_URL = os.environ["STAGING_BASE_URL"]
AUTH_BASE_URL = f"{BASE_URL}/{AUTH_PATH}"
AUTH_ID_TOKEN = (
    os.environ.get("AUTH_ID_TOKEN")
    or os.environ.get("COGNITO_ID_TOKEN")
    or os.environ.get("STAGING_AUTH_ID_TOKEN")
)


def _auth_headers():
    if not AUTH_ID_TOKEN:
        pytest.skip(
            "Set AUTH_ID_TOKEN, COGNITO_ID_TOKEN, or STAGING_AUTH_ID_TOKEN "
            "to run authenticated auth tests"
        )
    return {"Authorization": f"Bearer {AUTH_ID_TOKEN}"}


def test_auth_profile_rejects_missing_token():
    response = requests.get(f"{AUTH_BASE_URL}/profile", timeout=10)

    assert response.status_code == STATUS_UNAUTHORIZED


def test_auth_profile_rejects_invalid_token():
    response = requests.get(
        f"{AUTH_BASE_URL}/profile",
        headers={"Authorization": "Bearer not-a-valid-jwt"},
        timeout=10,
    )

    assert response.status_code == STATUS_UNAUTHORIZED


def test_auth_profile_update_rejects_missing_token():
    response = requests.put(
        f"{AUTH_BASE_URL}/profile",
        json={"company_name": "System Test Logistics"},
        timeout=10,
    )

    assert response.status_code == STATUS_UNAUTHORIZED


def test_auth_password_update_rejects_missing_token():
    response = requests.put(
        f"{AUTH_BASE_URL}/password",
        json={
            "current_password": "OldPassword123!",
            "new_password": "NewPassword123!",
        },
        timeout=10,
    )

    assert response.status_code == STATUS_UNAUTHORIZED


def test_auth_profile_valid_token_returns_profile():
    response = requests.get(
        f"{AUTH_BASE_URL}/profile",
        headers=_auth_headers(),
        timeout=10,
    )

    assert response.status_code == STATUS_OK
    profile = response.json()
    assert set(profile) == {"username", "email", "email_verified", "company_name"}
    assert isinstance(profile["username"], str)
    assert isinstance(profile["email_verified"], bool)


def test_auth_profile_update_rejects_unsupported_fields_with_valid_token():
    response = requests.put(
        f"{AUTH_BASE_URL}/profile",
        headers=_auth_headers(),
        json={"ignored": "value"},
        timeout=10,
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {"error": "No supported profile fields provided"}


def test_auth_password_update_rejects_missing_fields_with_valid_token():
    response = requests.put(
        f"{AUTH_BASE_URL}/password",
        headers=_auth_headers(),
        json={},
        timeout=10,
    )

    assert response.status_code == STATUS_BAD_REQUEST
    assert response.json() == {
        "error": "current_password and new_password are required"
    }
