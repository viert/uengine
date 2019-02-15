import os

from datetime import datetime
from bson.objectid import ObjectId, InvalidId
from flask import json, make_response
from uuid import uuid4

from . import ctx


def get_py_files(directory):
    return [f for f in os.listdir(directory) if f.endswith(".py")]


def get_modules(directory):
    return [x[:-3] for x in get_py_files(directory) if x != "__init__.py"]


def resolve_id(id_):
    # ObjectId(None) apparently generates a new unique object id
    # which is not a behaviour we need
    if id_ is None:
        return None
    try:
        objid_expr = ObjectId(id_)
        if str(objid_expr) == id_:
            id_ = objid_expr
    except InvalidId:
        pass
    return id_


def cursor_to_list(crs, fields=None):
    return [x.to_dict(fields) for x in crs]


def clear_aux_fields(data):
    return {k: v for k, v in data.items() if not k.startswith("_")}


def uuid4_string():
    return str(uuid4())


def json_response(data, code=200):
    json_kwargs = {}
    if ctx.cfg.get("debug"):
        json_kwargs["indent"] = 4
    return make_response(json.dumps(data, **json_kwargs), code, {'Content-Type': 'application/json'})


# For some reason Mongo stores datetime rounded to milliseconds
# Following is useful to avoid inconsistencies in unit tests
# - Roman Andriadi
def now():
    dt = datetime.utcnow()
    dt = dt.replace(microsecond=dt.microsecond//1000*1000)
    return dt


NilObjectId = ObjectId("000000000000000000000000")
