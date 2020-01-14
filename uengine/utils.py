import os
from flask import has_request_context, g
from datetime import datetime
from bson.objectid import ObjectId, InvalidId
from urllib.parse import urlencode, unquote, urlparse, parse_qsl, ParseResult
from uuid import uuid4


def get_py_files(directory):
    return [f for f in os.listdir(directory) if f.endswith(".py")]


def get_modules(directory):
    return [x[:-3] for x in get_py_files(directory) if x != "__init__.py"]


def resolve_id(id_):
    # ObjectId(None) apparently generates a new unique object id
    # which is not a behaviour we need
    if id_ is not None:
        try:
            objid_expr = ObjectId(id_)
            if str(objid_expr) == id_:
                id_ = objid_expr
        except (InvalidId, TypeError):
            pass
    return id_


def cursor_to_list(crs, fields=None):
    return [x.to_dict(fields) for x in crs]


def clear_aux_fields(data):
    return {k: v for k, v in data.items() if not k.startswith("_")}


def uuid4_string():
    return str(uuid4())


def get_user_from_app_context():
    if not has_request_context():
        return None
    return g.user


# Mongo stores datetime rounded to milliseconds as it's date abilities are powered by V8
# Following is useful to avoid inconsistencies in unit tests
def now():
    dt = datetime.utcnow()
    dt = dt.replace(microsecond=dt.microsecond//1000*1000)
    return dt


NilObjectId = ObjectId("000000000000000000000000")


def check_dicts_are_equal(dict1, dict2):
    """deep dict compare helper"""
    if dict1 == dict2:
        # the same object
        return True

    if len(dict1) != len(dict2):
        return False

    for k, v in dict1.items():
        if k not in dict2:
            return False
        if v == dict2[k]:
            continue
        if type(v) != type(dict2[k]):
            return False
        if type(v) is dict and check_dicts_are_equal(v, dict2[k]):
            continue
        if type(v) is list and check_lists_are_equal(v, dict2[k]):
            continue
        return False

    return True


def check_lists_are_equal(list1, list2):
    """deep list compare helper"""
    if list1 == list2:
        # the same object
        return True

    if len(list1) != len(list2):
        return False

    for i, e in enumerate(list1):
        if e == list2[i]:
            continue
        if type(e) != type(list2[i]):
            return False
        if type(e) is dict and check_dicts_are_equal(e, list2[i]):
            continue
        if type(e) is list and check_lists_are_equal(e, list2[i]):
            continue
        return False


def add_url_params(url, params):
    """ Add GET params to provided URL being aware of existing.

    :param url: string of target URL
    :param params: dict containing requested params to be added
    :return: string with updated URL

    >> url = 'http://stackoverflow.com/test?answers=true'
    >> new_params = {'answers': False, 'data': ['some','values']}
    >> add_url_params(url, new_params)
    'http://stackoverflow.com/test?data=some&data=values&answers=false'

    Taken from https://stackoverflow.com/questions/2506379/add-params-to-given-url-in-python
    Thanks to Sapphire64
    """
    url = unquote(url)
    parsed_url = urlparse(url)
    get_args = parsed_url.query
    parsed_get_args = dict(parse_qsl(get_args))
    parsed_get_args.update(params)
    parsed_get_args.update(
        {k: json.dumps(v) for k, v in parsed_get_args.items()
         if isinstance(v, (bool, dict))}
    )
    encoded_get_args = urlencode(parsed_get_args, doseq=True)
    new_url = ParseResult(
        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
        parsed_url.params, encoded_get_args, parsed_url.fragment
    ).geturl()
    return new_url
