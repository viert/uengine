from traceback import format_exc
from werkzeug.exceptions import HTTPException

from . import ctx
from .api import json_response


class ApiError(Exception):

    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code:
            self.status_code = status_code
        self.payload = payload or {}

    def to_dict(self):
        data = self.payload
        data["error"] = self.message
        return data

    def __repr__(self):
        return "%s: %s, status_code=%s" % (self.__class__.__name__, self.message, self.status_code)

    def __str__(self):
        return "%s, status_code=%s" % (self.message, self.status_code)


class AuthenticationError(ApiError):

    status_code = 401
    auth_url = None

    def __init__(self, message="you must be authenticated first", payload=None):
        ApiError.__init__(self, message, payload=payload)
        if self.auth_url is None:
            oauth_cfg = ctx.cfg.get("oauth")
            if oauth_cfg:
                client_id = oauth_cfg.get("id")
                auth_url = oauth_cfg.get("authorize_url")
                callback_url = oauth_cfg.get("callback_url")
                if client_id and auth_url and callback_url:
                    AuthenticationError.auth_url = f"{auth_url}?response_type=code&" \
                        f"client_id={client_id}&scope=user_info&redirect_uri={callback_url}"

    def to_dict(self):
        data = super().to_dict()
        data["oauth"] = self.auth_url
        if "state" not in data:
            data["state"] = "logged out"
        return data


class ConfigurationError(SystemExit):
    pass


class Forbidden(ApiError):
    status_code = 403


class IntegrityError(ApiError):
    status_code = 409


class NotFound(ApiError):
    status_code = 404


class InvalidShardId(ApiError):
    status_code = 500


class ShardIsReadOnly(IntegrityError):
    pass


class ModelDestroyed(IntegrityError):
    pass


class MissingSubmodel(IntegrityError):
    pass


class WrongSubmodel(IntegrityError):
    pass


class UnknownSubmodel(IntegrityError):
    pass


class InputDataError(ApiError):
    pass


class InvalidFieldType(ApiError):
    pass


def handle_api_error(error):
    return json_response(error.to_dict(), error.status_code)


def handle_other_errors(error):
    code = 400
    if hasattr(error, 'code'):
        code = error.code
    if not (100 <= code < 600):
        code = 400
    if not issubclass(error.__class__, HTTPException):
        ctx.log.error(format_exc())
    return json_response({"error": str(error)}, code)
