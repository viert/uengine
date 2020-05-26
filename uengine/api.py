import functools
import re

from collections import namedtuple
from math import ceil
from flask import request, json, make_response

from . import ctx

ArithmeticExpression = namedtuple(
    '_ArithmeticExpression', field_names=["op", "value"])

DEFAULT_DOCUMENTS_PER_PAGE = 10
ARITHMETIC_OPS = (
    "eq",
    "lt",
    "gt",
    "lte",
    "gte",
    "ne",
)
INTEGER_EXPRESSION = re.compile("^\d+$")


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
    default_limit = ctx.cfg.get(
        'documents_per_page', DEFAULT_DOCUMENTS_PER_PAGE)
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
        raise RuntimeError(
            "paginated() accepts either cursor objects or lists")

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


def filter_expr(flt):
    if flt.find(" ") >= 0:
        tokens = flt.split()
        return {"$in": tokens}
    try:
        return re.compile(flt, re.IGNORECASE)
    # re.compile's can throw multiple different exceptions. We do not care what went wrong
    except Exception:  # pylint: disable=broad-except
        return ""


def arithmetic_expr(expr, try_int=True):
    tokens = expr.split(":")
    expr_op = "eq"
    if len(tokens) == 2 and tokens[0] in ARITHMETIC_OPS:
        expr_op = tokens[0]
        expr = tokens[1]

    if try_int and INTEGER_EXPRESSION.match(expr):
        expr = int(expr)

    return ArithmeticExpression(op=expr_op, value=expr)


def json_body_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        from .errors import InputDataError
        if request.json is None:
            raise InputDataError("json data is missing")
        return func(*args, **kwargs)
    return wrapper


def json_response(data, code=200):
    json_kwargs = {}
    if ctx.cfg.get("debug"):
        json_kwargs["indent"] = 4
    return make_response(json.dumps(data, **json_kwargs), code, {'Content-Type': 'application/json'})
