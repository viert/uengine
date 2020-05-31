from typing import Any
from copy import deepcopy

from bson.objectid import ObjectId
from functools import wraps
from itertools import chain
from pymongo import ASCENDING, DESCENDING, HASHED
from pymongo.errors import OperationFailure
from uengine import ctx
from uengine.errors import ApiError, InvalidFieldType
from .model_hook import ModelHook


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
        ApiError.__init__(self, "Field \"%s\" is required" %
                          field_name, status_code=400)


# special exception to silently avoid saving
# in _before_save
class DoNotSave(Exception):
    pass


def merge_set(attr, new_cls, bases):
    merged = set()
    valid_types = (list, set, frozenset, tuple)
    for cls in chain(bases, [new_cls]):
        cls_attr = getattr(cls, attr, [])
        if not isinstance(cls_attr, valid_types):
            raise TypeError(
                "{} field must be one of: {}".format(attr, valid_types))
        merged.update(cls_attr)

    setattr(new_cls, attr, frozenset(merged))


def merge_tuple(attr, new_cls, bases):
    merged = []
    for cls in chain(bases, [new_cls]):
        cls_attr = getattr(cls, attr, [])
        if not isinstance(cls_attr, (list, set, tuple)):
            raise TypeError(
                "{} field must be of type list, set or tuple".format(attr))
        merged.extend(cls_attr)
    setattr(new_cls, attr, tuple(merged))


def merge_dict(attr, new_cls, bases):
    merged = {}
    for cls in chain(bases, [new_cls]):
        cls_attr = getattr(cls, attr, {})
        if not isinstance(cls_attr, dict):
            raise TypeError("{} field must be a dict".format(attr))
        merged.update(cls_attr)
    setattr(new_cls, attr, merged)


class ModelMeta(type):

    def __new__(mcs, name, bases, dct) -> Any:
        new_cls = super().__new__(mcs, name, bases, dct)

        # First merge mergers config
        merge_dict("_MERGERS", new_cls, bases)

        for attr, merge_func in new_cls._MERGERS.items():
            merge_func(attr, new_cls, bases)

        compatibility_fields = []
        for field in new_cls.FIELDS:
            if field in new_cls.COMPATIBILITY_FIELD_MAP:
                compatibility_fields.append(
                    new_cls.COMPATIBILITY_FIELD_MAP[field])
            else:
                compatibility_fields.append(field)
        new_cls.COMPATIBILITY_FIELDS = frozenset(compatibility_fields)

        new_cls.collection = mcs._get_collection(new_cls, name, bases, dct)

        return new_cls

    @staticmethod
    def _get_collection(model_cls, name, bases, dct):  # pylint: disable=unused-argument
        # Do not inherit collection names from base classes
        if 'COLLECTION' in dct:
            return dct['COLLECTION']
        return snake_case(name)


class AbstractModel(metaclass=ModelMeta):

    FIELDS = [
        "_id",
    ]
    REJECTED_FIELDS = []
    REQUIRED_FIELDS = set()
    RESTRICTED_FIELDS = []
    AUTO_TRIM_FIELDS = []
    KEY_FIELD = None
    DEFAULTS = {}
    VALIDATION_TYPES = {}
    INDEXES = []
    COMPATIBILITY_FIELD_MAP = {}

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

    _MERGERS = {
        "FIELDS": merge_set,
        "REJECTED_FIELDS": merge_set,
        "REQUIRED_FIELDS": merge_set,
        "RESTRICTED_FIELDS": merge_set,
        "AUXILIARY_SLOTS": merge_set,
        "DEFAULTS": merge_dict,
        "VALIDATION_TYPES": merge_dict,
        "INDEXES": merge_tuple,
    }

    _HOOKS = None

    __hash__ = None
    __slots__ = FIELDS + ["_id", "_hooks"]

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
        self.__set_initial_state()
        self._hooks = []
        if self._HOOKS:
            for hook_class in self._HOOKS:
                hook_inst = hook_class.on_model_init(self)
                if hook_inst:
                    self._hooks.append(hook_inst)

    def __set_initial_state(self):
        setattr(self, "_initial_state", deepcopy(self.to_dict(self.FIELDS)))

    @classmethod
    def register_model_hook(cls, model_hook_class, *args, **kwargs):
        if not issubclass(model_hook_class, ModelHook):
            raise TypeError("Invalid hook class")
        if cls._HOOKS is None:
            cls._HOOKS = set()
        if model_hook_class not in cls._HOOKS:
            cls._HOOKS.add(model_hook_class)
            model_hook_class.on_hook_register(cls, *args, **kwargs)
            ctx.log.debug("Registered hook %s for model %s",
                          model_hook_class.__name__, cls.__name__)

    @classmethod
    def unregister_model_hook(cls, model_hook_class):
        if cls._HOOKS is None:
            return
        if model_hook_class in cls._HOOKS:
            cls._HOOKS.remove(model_hook_class)
            model_hook_class.on_hook_unregister(cls)

    @classmethod
    def clear_hooks(cls):
        if cls._HOOKS is None:
            return
        for hook_class in cls._HOOKS.copy():
            cls.unregister_model_hook(hook_class)

    def _before_save(self):
        pass

    def _before_validation(self):
        pass

    def _before_delete(self):
        pass

    def _after_save(self, is_new):
        pass

    def _after_delete(self):
        pass

    def _save_to_db(self):
        pass

    def _delete_from_db(self):
        pass

    def invalidate(self):
        pass

    def _validate(self):
        for field in self.missing_fields:
            raise FieldRequired(field)

        for field_name, f_type in self.VALIDATION_TYPES.items():
            if not isinstance(getattr(self, field_name), f_type):
                raise InvalidFieldType(
                    f"field {field_name} must be of type {f_type.__name__}")

    def _reload_from_obj(self, obj):
        for field in self.FIELDS:
            if field == "_id":
                continue
            value = getattr(obj, field)
            setattr(self, field, value)

    def destroy(self, skip_callback=False, invalidate_cache=True):
        if self.is_new:
            return
        if not skip_callback:
            self._before_delete()
        self._delete_from_db()
        if not skip_callback:
            self._after_delete()
        old_id = self._id
        self._id = None
        for hook in self._hooks:
            try:
                hook.on_model_destroy(self)
            except Exception as e:
                ctx.log.error("error executing destroy hook %s on model %s(%s): %s",
                              hook.__class__.__name__, self.__class__.__name__, self._id, e)
        if invalidate_cache:
            self.invalidate(_id=old_id)
        return self

    def save(self, skip_callback=False, invalidate_cache=True):
        is_new = self.is_new

        if not skip_callback:
            try:
                self._before_validation()
            except DoNotSave:
                return

        self._validate()

        # autotrim
        for field in self.AUTO_TRIM_FIELDS:
            value = getattr(self, field)
            try:
                value = value.strip()
                setattr(self, field, value)
            except AttributeError:
                pass

        if not skip_callback:
            try:
                self._before_save()
            except DoNotSave:
                return
        self._save_to_db()

        for hook in self._hooks:
            try:
                hook.on_model_save(self, is_new)
            except Exception as e:
                ctx.log.error("error executing save hook %s on model %s(%s): %s",
                              hook.__class__.__name__, self.__class__.__name__, self._id, e)

        self.__set_initial_state()
        if invalidate_cache:
            self.invalidate()
        if not skip_callback:
            self._after_save(is_new)

        return self

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

    @classmethod
    def from_data(cls, **data):
        return cls(**data)

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
                ctx.log.debug(
                    "Creating index with options: %s, %s", keys, options)

            for db in cls._get_possible_databases():
                try:
                    db.conn[cls.collection].create_index(keys, **options)
                except OperationFailure as e:
                    if e.details.get("codeName") == "IndexOptionsConflict" or e.details.get("code") == 85:
                        if overwrite:
                            if loud:
                                ctx.log.debug(
                                    "Dropping index %s as conflicting", keys)
                            db.conn[cls.collection].drop_index(keys)
                            if loud:
                                ctx.log.debug(
                                    "Creating index with options: %s, %s", keys, options)
                            db.conn[cls.collection].create_index(
                                keys, **options)
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
