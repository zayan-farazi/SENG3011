import pytest

import constants
from auth_context import AuthError, get_jwt_claims, require_authenticated_user


def auth_event(claims=None):
    return {
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": claims
                    or {
                        "sub": "user-123",
                        "email": "user@example.com",
                        "email_verified": "true",
                    }
                }
            }
        }
    }


def test_get_jwt_claims_extracts_claims():
    claims = get_jwt_claims(auth_event())

    assert claims == {
        "sub": "user-123",
        "email": "user@example.com",
        "email_verified": "true",
    }


def test_require_authenticated_user_requires_sub():
    with pytest.raises(AuthError) as error:
        require_authenticated_user({})

    assert error.value.status_code == constants.STATUS_UNAUTHORIZED
    assert error.value.message == "Unauthorized"


def test_require_authenticated_user_requires_verified_email():
    with pytest.raises(AuthError) as error:
        require_authenticated_user(
            auth_event(
                {
                    "sub": "user-123",
                    "email": "user@example.com",
                    "email_verified": "false",
                }
            ),
            require_verified_email=True,
        )

    assert error.value.status_code == constants.STATUS_FORBIDDEN
    assert error.value.message == "Verified email required"
