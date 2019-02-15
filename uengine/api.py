import functools
import re

from math import ceil
from flask import request, g

from . import ctx
from .errors import InputDataError

DEFAULT_DOCUMENTS_PER_PAGE = 10


def get_page(nopaging=False):
    if nopaging:
        return None
    if '_page' in request.values:
        page = request.values['_page']
    else:
        page = 1
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    return page


def get_limit(nopaging=False):
    if nopaging:
        return None
    default_limit = ctx.cfg.get('documents_per_page', DEFAULT_DOCUMENTS_PER_PAGE)
    if '_limit' in request.values:
        limit = request.values['_limit']
        try:
            limit = int(limit)
        except ValueError:
            limit = default_limit
    else:
        limit = default_limit
    return limit


def get_request_fields(default_fields=None):
    if "_fields" in request.values:
        return request.values["_fields"].split(",")
    return default_fields


def boolean(value):
    return str(value).lower() in ("yes", "true", "1")


def get_boolean_request_param(param_name):
    return boolean(request.values.get(param_name))


def paginated(data, page=None, limit=None, extra=None, transform=None):

    nopaging = get_boolean_request_param("_nopaging")
    if page is None:
        page = get_page(nopaging)
    if limit is None:
        limit = get_limit()

    if isinstance(data, list):
        count = len(data)
        if limit is not None and page is not None:
            data = data[(page - 1) * limit:page * limit]
    elif hasattr(data, "count"):
        count = data.count()
        if limit is not None and page is not None:
            data = data.skip((page-1)*limit).limit(limit)
    else:
        raise RuntimeError("paginated() accepts either cursor objects or lists")

    if transform is not None:
        data = [transform(x) for x in data]

    total_pages = ceil(count / limit) if limit is not None else None

    result = {
        "page": page,
        "total_pages": total_pages,
        "count": count,
        "data": data
    }

    if extra is not None and hasattr(extra, "items"):
        for k, v in extra.items():
            if k not in result:
                result[k] = v

    return result


def default_transform(fields=None):
    def transform(x):
        return x.to_dict(fields=get_request_fields(fields))
    return transform


def get_user_from_app_context():
    user = None
    try:
        user = g.user
    except AttributeError:
        pass
    # except RuntimeError:
    #     raise OutsideApplicationContext("trying to get g object outside app context")
    return user


def filter_expr(flt):
    try:
        return re.compile(flt, re.IGNORECASE)
    # re.compile's can throw multiple different exceptions. We do not care what went wrong
    except Exception:  # pylint: disable=broad-except
        return ""


def json_body_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        if request.json is None:
            raise InputDataError("json data is missing")
        return func(*args, **kwargs)
    return wrapper


def parse_oauth_state(state_expr):
    results = {}
    states = state_expr.split("|")
    for state in states:
        tokens = state.split(":")
        if len(tokens) == 2:
            results[tokens[0]] = tokens[1]
    return results
