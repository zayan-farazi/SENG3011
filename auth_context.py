import json

import constants


class AuthError(Exception):
    def __init__(self, status_code, message):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def get_jwt_claims(event):
    request_context = event.get("requestContext") or {}
    authorizer = request_context.get("authorizer") or {}
    jwt = authorizer.get("jwt") or {}
    claims = jwt.get("claims") or {}
    return claims if isinstance(claims, dict) else {}


def require_authenticated_user(event, require_verified_email=False):
    claims = get_jwt_claims(event)
    user_id = claims.get("sub")
    if not user_id:
        raise AuthError(constants.STATUS_UNAUTHORIZED, "Unauthorized")

    email = claims.get("email")
    email_verified = str(claims.get("email_verified", "")).lower() == "true"
    if require_verified_email and (not email or not email_verified):
        raise AuthError(constants.STATUS_FORBIDDEN, "Verified email required")

    return {
        "user_id": user_id,
        "notification_email": email,
        "email_verified": email_verified,
        "username": claims.get("cognito:username"),
    }


def auth_error_response(error):
    return {
        "statusCode": error.status_code,
        "body": json.dumps({"error": error.message}),
    }
