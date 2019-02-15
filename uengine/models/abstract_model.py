from bson.objectid import ObjectId
from functools import wraps
from pymongo import ASCENDING, DESCENDING, HASHED
from pymongo.errors import OperationFailure
from uengine import ctx
from uengine.errors import ApiError, InvalidFieldType


def snake_case(name):
    result = ""
    for i, l in enumerate(name):
        if 65 <= ord(l) <= 90:
            if i != 0:
                result += "_"
            result += l.lower()
        else:
            result += l
    return result


def parse_index_key(index_key):
    if index_key.startswith("-"):
        index_key = index_key[1:]
        order = DESCENDING
    elif index_key.startswith("#"):
        index_key = index_key[1:]
        order = HASHED
    else:
        order = ASCENDING
        if index_key.startswith("+"):
            index_key = index_key[1:]
    return index_key, order


class ObjectSaveRequired(Exception):
    pass


class FieldRequired(ApiError):
    def __init__(self, field_name):
        ApiError.__init__(self, "Field \"%s\" is required" % field_name, status_code=400)


class ModelMeta(type):
    _collection = None

    @property
    def collection(cls):
        if cls._collection is None:
            cls._collection = snake_case(cls.__name__)
        return cls._collection


class AbstractModel(metaclass=ModelMeta):

    FIELDS = []
    REJECTED_FIELDS = []
    REQUIRED_FIELDS = set()
    RESTRICTED_FIELDS = []
    KEY_FIELD = None
    DEFAULTS = {}
    VALIDATION_TYPES = {}
    INDEXES = []

    AUXILIARY_SLOTS = (
        "AUXILIARY_SLOTS",
        "FIELDS",
        "REJECTED_FIELDS",
        "REQUIRED_FIELDS",
        "RESTRICTED_FIELDS",
        "KEY_FIELD",
        "DEFAULTS",
        "INDEXES",
    )

    __hash__ = None
    __slots__ = FIELDS + ["_id"]

    def __init__(self, **kwargs):
        if "_id" not in kwargs:
            self._id = None
        for field, value in kwargs.items():
            if field in self.FIELDS:
                setattr(self, field, value)
        for field in self.FIELDS:
            if field not in kwargs:
                value = self.DEFAULTS.get(field)
                if callable(value):
                    value = value()
                elif hasattr(value, "copy"):
                    value = value.copy()
                elif hasattr(value, "__getitem__"):
                    value = value[:]
                setattr(self, field, value)

    def _before_save(self):
        pass

    def _before_delete(self):
        pass

    def _after_save(self):
        pass

    def _after_delete(self):
        pass

    def _save_to_db(self):
        pass

    def _validate(self):
        for field, f_type in self.VALIDATION_TYPES.items():
            if not isinstance(getattr(self, field), f_type):
                raise InvalidFieldType(f"field {field} must be of type {f_type.__name__}")

    def save(self, skip_callback=False):
        for field in self.missing_fields:
            raise FieldRequired(field)
        self._validate()
        if not skip_callback:
            self._before_save()
        self._save_to_db()
        if not skip_callback:
            self._after_save()

    def __repr__(self):
        attributes = ["%s=%r" % (a, getattr(self, a))
                      for a in list(self.FIELDS)]
        return '%s(\n    %s\n)' % (self.__class__.__name__, ',\n    '.join(attributes))

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        for field in self.FIELDS:
            if hasattr(self, field):
                if not hasattr(other, field):
                    return False
                if getattr(self, field) != getattr(other, field):
                    return False
            elif hasattr(other, field):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_dict(self, fields=None, include_restricted=False):
        if fields is None:
            fields = list(self.FIELDS)
        result = {}
        for field in fields:
            if field.startswith("_") and field != "_id":
                continue
            if field in self.AUXILIARY_SLOTS:
                continue
            if field in self.RESTRICTED_FIELDS and not include_restricted:
                continue
            try:
                value = getattr(self, field)
            except AttributeError:
                continue
            if callable(value):
                continue
            result[field] = value
        return result

    @property
    def collection(self):
        return self.__class__.collection

    @property
    def is_complete(self):
        return len(self.missing_fields) == 0

    @property
    def is_new(self):
        return not (hasattr(self, "_id") and isinstance(self._id, ObjectId))

    @property
    def missing_fields(self):
        mfields = []
        for field in self.REQUIRED_FIELDS:
            if not hasattr(self, field) or getattr(self, field) in ["", None]:
                mfields.append(field)
        return mfields

    @classmethod
    def _get_possible_databases(cls):
        return [ctx.db.meta]

    @classmethod
    def ensure_indexes(cls, loud=False, overwrite=False):  # pylint: disable=too-many-branches
        if not isinstance(cls.INDEXES, (list, tuple)):
            raise TypeError("INDEXES field must be of type list or tuple")

        for index in cls.INDEXES:
            if isinstance(index, str):
                index = [index]
            keys = []
            options = {"sparse": False}

            for sub_index in index:
                if isinstance(sub_index, str):
                    keys.append(parse_index_key(sub_index))
                else:
                    for key, value in sub_index.items():
                        options[key] = value
            if loud:
                ctx.log.debug("Creating index with options: %s, %s", keys, options)

            for db in cls._get_possible_databases():
                try:
                    db.conn[cls.collection].create_index(keys, **options)
                except OperationFailure as e:
                    if e.details.get("codeName") == "IndexOptionsConflict" or e.details.get("code") == 85:
                        if overwrite:
                            if loud:
                                ctx.log.debug("Dropping index %s as conflicting", keys)
                            db.conn[cls.collection].drop_index(keys)
                            if loud:
                                ctx.log.debug("Creating index with options: %s, %s", keys, options)
                            db.conn[cls.collection].create_index(keys, **options)
                        else:
                            ctx.log.error(
                                "Index %s conflicts with an existing one, use overwrite param to fix it", keys
                            )


def save_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        this = args[0]
        if this.is_new:
            raise ObjectSaveRequired("This object must be saved first")
        return func(*args, **kwargs)
    return wrapper
