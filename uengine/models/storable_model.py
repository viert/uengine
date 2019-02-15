from uengine import ctx
from uengine.utils import resolve_id
from uengine.errors import NotFound, ModelDestroyed
from uengine.cache import req_cache_get, req_cache_set, req_cache_has_key, req_cache_delete
from datetime import datetime

from .abstract_model import AbstractModel


class StorableModel(AbstractModel):

    def __init__(self, **kwargs):
        AbstractModel.__init__(self, **kwargs)

    def _save_to_db(self):
        ctx.db.meta.save_obj(self)

    def update(self, data, skip_callback=False):
        for field in self.FIELDS:
            if field in data and field not in self.REJECTED_FIELDS and field != "_id":
                self.__setattr__(field, data[field])
        self.save(skip_callback=skip_callback)

    def destroy(self, skip_callback=False):
        if self.is_new:
            return
        if not skip_callback:
            self._before_delete()
        ctx.db.meta.delete_obj(self)
        if not skip_callback:
            self._after_delete()
        self._id = None

    def reload(self):
        tmp = self.__class__.find_one({"_id": self._id})
        if tmp is None:
            raise ModelDestroyed("model has been deleted from db")
        for field in self.FIELDS:
            if field == "_id":
                continue
            value = getattr(tmp, field)
            setattr(self, field, value)

    @classmethod
    def find(cls, query=None, **kwargs):
        if not query:
            query = {}
        return ctx.db.meta.get_objs(cls, cls.collection, query, **kwargs)

    @classmethod
    def find_one(cls, query, **kwargs):
        return ctx.db.meta.get_obj(cls, cls.collection, query, **kwargs)

    @classmethod
    def get(cls, expression, raise_if_none=None):
        from bson.objectid import ObjectId

        expression = resolve_id(expression)
        if isinstance(expression, ObjectId):
            query = {"_id": expression}
        else:
            expression = str(expression)
            query = {cls.KEY_FIELD: expression}
        res = cls.find_one(query)
        if res is None and raise_if_none is not None:
            if isinstance(raise_if_none, Exception):
                raise raise_if_none
            else:
                raise NotFound(f"{cls.__name__} not found")
        return res

    @classmethod
    def cache_get(cls, expression, raise_if_none=None):
        cache_key = f"{cls.__name__}.{expression}"
        d1 = datetime.now()

        if req_cache_has_key(cache_key):
            data = req_cache_get(cache_key)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug(f"ModelCache L1 HIT {cache_key} {td:.3f} seconds")
            return cls(**data)

        if ctx.cache.has(cache_key):
            data = ctx.cache.get(cache_key)
            req_cache_set(cache_key, data)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug(f"ModelCache L2 HIT {cache_key} {td:.3f} seconds")
            return cls(**data)

        instance = cls.get(expression, raise_if_none)
        if instance is not None:
            data = instance.to_dict()
            ctx.cache.set(cache_key, data)
            req_cache_set(cache_key, data)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug(f"ModelCache MISS {cache_key} {td:.3f} seconds")
        return instance

    def invalidate(self):
        cache_key_id = f"{self.__class__.__name__}.{self._id}"
        cache_key_keyfield = None
        if self.KEY_FIELD is not None and self.KEY_FIELD != "_id":
            cache_key_keyfield = f"{self.__class__.__name__}.{getattr(self, self.KEY_FIELD)}"

        cr_layer1_id = req_cache_delete(cache_key_id)
        cr_layer2_id = ctx.cache.delete(cache_key_id)
        cr_layer1_keyfield = None
        cr_layer2_keyfield = None
        if cache_key_keyfield:
            cr_layer1_keyfield = req_cache_delete(cache_key_keyfield)
            cr_layer2_keyfield = ctx.cache.delete(cache_key_keyfield)

        return cr_layer1_id, cr_layer1_keyfield, cr_layer2_id, cr_layer2_keyfield

    @classmethod
    def destroy_all(cls):
        ctx.db.meta.delete_query(cls.collection, {})

    @classmethod
    def destroy_many(cls, query):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.meta.delete_query(cls.collection, query)

    @classmethod
    def update_many(cls, query, attrs):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.meta.update_query(cls.collection, query, attrs)
