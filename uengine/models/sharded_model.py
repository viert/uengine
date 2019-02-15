from bson.objectid import ObjectId
from datetime import datetime
from uengine import ctx
from uengine.errors import ApiError, NotFound, ModelDestroyed
from uengine.utils import resolve_id
from uengine.cache import req_cache_has_key, req_cache_get, req_cache_set, req_cache_delete

from .abstract_model import AbstractModel


class MissingShardId(ApiError):
    status_code = 500


class ShardedModel(AbstractModel):

    def __init__(self, **kwargs):
        AbstractModel.__init__(self, **kwargs)
        self._shard_id = None
        if "shard_id" in kwargs:
            self._shard_id = kwargs["shard_id"]
        if not self.is_new and self._shard_id is None:
            from traceback import print_stack
            print_stack()
            raise MissingShardId("ShardedModel from database with missing shard_id - this must be a bug")

    def _save_to_db(self):
        ctx.db.shards[self._shard_id].save_obj(self)

    def save(self, skip_callback=False):
        if self._shard_id is None:
            raise MissingShardId("ShardedModel must have shard_id set before save")
        super().save(skip_callback)

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
        ctx.db.shards[self._shard_id].delete_obj(self)
        if not skip_callback:
            self._after_delete()
        self._id = None

    def reload(self):
        if self.is_new:
            return
        tmp = self.__class__.find_one(self._shard_id, {"_id": self._id})
        if tmp is None:
            raise ModelDestroyed("model has been deleted from db")
        for field in self.FIELDS:
            if field == "_id":
                continue
            value = getattr(tmp, field)
            setattr(self, field, value)

    @classmethod
    def _get_possible_databases(cls):
        return list(ctx.db.shards.values())

    @classmethod
    def find(cls, shard_id, query=None, **kwargs):
        if not query:
            query = {}
        return ctx.db.get_shard(shard_id).get_objs(cls, cls.collection, query, **kwargs)

    @classmethod
    def find_one(cls, shard_id, query, **kwargs):
        return ctx.db.get_shard(shard_id).get_obj(cls, cls.collection, query, **kwargs)

    @classmethod
    def get(cls, shard_id, expression, raise_if_none=None):
        expression = resolve_id(expression)
        if isinstance(expression, ObjectId):
            query = {"_id": expression}
        else:
            expression = str(expression)
            query = {cls.KEY_FIELD: expression}
        res = cls.find_one(shard_id, query)
        if res is None and raise_if_none is not None:
            if isinstance(raise_if_none, Exception):
                raise raise_if_none
            else:
                raise NotFound(f"{cls.__name__} not found")
        return res

    @classmethod
    def cache_get(cls, shard_id, expression, raise_if_none=None):
        cache_key = f"{cls.__name__}.{shard_id}.{expression}"
        d1 = datetime.now()

        if req_cache_has_key(cache_key):
            data = req_cache_get(cache_key)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug("ModelCache L1 HIT %s %.3f seconds", cache_key, td)
            return cls(shard_id=shard_id, **data)

        if ctx.cache.has(cache_key):
            data = ctx.cache.get(cache_key)
            req_cache_set(cache_key, data)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug("ModelCache L2 HIT %s %.3f seconds", cache_key, td)
            return cls(shard_id=shard_id, **data)

        instance = cls.get(shard_id, expression, raise_if_none)
        if instance is not None:
            data = instance.to_dict()
            ctx.cache.set(cache_key, data)
            req_cache_set(cache_key, data)
            td = (datetime.now() - d1).total_seconds()
            ctx.log.debug(f"ModelCache MISS %s %.3f seconds", cache_key, td)
        return instance

    def invalidate(self):
        cache_key_id = f"{self.__class__.__name__}.{self._shard_id}.{self._id}"
        cache_key_keyfield = None
        if self.KEY_FIELD is not None and self.KEY_FIELD != "_id":
            cache_key_keyfield = f"{self.__class__.__name__}.{self._shard_id}.{getattr(self, self.KEY_FIELD)}"

        cr_layer1_id = req_cache_delete(cache_key_id)
        cr_layer2_id = ctx.cache.delete(cache_key_id)
        cr_layer1_keyfield = None
        cr_layer2_keyfield = None
        if cache_key_keyfield:
            cr_layer1_keyfield = req_cache_delete(cache_key_keyfield)
            cr_layer2_keyfield = ctx.cache.delete(cache_key_keyfield)

        return cr_layer1_id, cr_layer1_keyfield, cr_layer2_id, cr_layer2_keyfield

    @classmethod
    def destroy_all(cls, shard_id):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).delete_query(cls.collection, {})

    @classmethod
    def destroy_many(cls, shard_id, query):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).delete_query(cls.collection, query)

    @classmethod
    def update_many(cls, shard_id, query, attrs):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).update_query(cls.collection, query, attrs)
