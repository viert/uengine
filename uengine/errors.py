from traceback import format_exc
from werkzeug.exceptions import HTTPException

from . import ctx
from .utils import json_response


class ApiError(Exception):

    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        data = {
            "error": self.message
        }
        if self.payload:
            data["data"] = self.payload
        return data

    def __repr__(self):
        return "%s: %s, status_code=%s" % (self.__class__.__name__, self.message, self.status_code)

    def __str__(self):
        return "%s, status_code=%s" % (self.message, self.status_code)


class AuthenticationError(ApiError):

    status_code = 401

    def __init__(self, message="You must be authenticated first"):
        ApiError.__init__(self, message)
        self.auth_urls = {}
        for key, config in ctx.cfg.get("oauth", {}).items():
            self.auth_urls[key] = {
                "id": config["id"],
                "redirect_uri": config["redirect_uri"]
            }

    def to_dict(self):
        return {
            "error": self.message,
            "state": "logged out",
            "oauth": self.auth_urls
        }


class Forbidden(ApiError):
    status_code = 403


class InviteRequired(Forbidden):

    def __init__(self, message="Invite is required"):
        Forbidden.__init__(self, message)

    def to_dict(self):
        return {
            "error": self.message,
            "state": "not invited",
        }


class IntegrityError(ApiError):
    status_code = 409


class NotFound(ApiError):
    status_code = 404


class InvalidShardId(ApiError):
    status_code = 500


class ShardIsReadOnly(IntegrityError):
    pass


class FriendAlreadyExists(IntegrityError):
    pass


class FriendDoesntExist(IntegrityError):
    pass


class ModelDestroyed(IntegrityError):
    pass


class InvalidStreamType(ApiError):
    pass


class InvalidStreamOwner(ApiError):
    pass


class InvalidStreamModerationType(ApiError):
    pass


class InvalidClubType(ApiError):
    pass


class AlreadySubscribed(IntegrityError):
    pass


class NotSubscribed(IntegrityError):
    pass


class InvalidPostType(ApiError):
    pass


class InvalidStreamId(ApiError):
    pass


class InvalidAuthorId(ApiError):
    pass


class InvalidUserId(ApiError):
    pass


class InvalidPostId(ApiError):
    pass


class InvalidParent(ApiError):
    pass


class InvalidLikeValue(ApiError):
    pass


class InvalidVisibilityType(ApiError):
    pass


class InputDataError(ApiError):
    pass


class InvalidTags(ApiError):
    pass


class InvalidFieldType(ApiError):
    pass


def handle_api_error(error):
    return json_response(error.to_dict(), error.status_code)


def handle_other_errors(error):
    code = 400
    if hasattr(error, 'code'):
        code = error.code
    if not issubclass(error.__class__, HTTPException):
        ctx.log.error(format_exc())
    return json_response({"error": str(error)}, code)
