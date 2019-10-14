from flask.json import JSONEncoder
from bson import ObjectId, Timestamp
from .models.abstract_model import AbstractModel
from .db import ObjectsCursor


class MongoJSONEncoder(JSONEncoder):
    def default(self, o):  # pylint: disable=method-hidden

        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Timestamp):
            return o.time
        if isinstance(o, (ObjectsCursor, set)):
            return list(o)
        if isinstance(o, AbstractModel):
            return o.to_dict()

        return JSONEncoder.default(self, o)
