from bson.objectid import ObjectId
from functools import partial
from uengine import ctx
from uengine.errors import ApiError, NotFound
from uengine.utils import resolve_id

from .storable_model import StorableModel


class MissingShardId(ApiError):
    status_code = 500


# pylint: disable=arguments-differ
class ShardedModel(StorableModel):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._shard_id = None
        if "shard_id" in kwargs:
            self._shard_id = kwargs["shard_id"]
        if not self.is_new and self._shard_id is None:
            from traceback import print_stack
            print_stack()
            raise MissingShardId(
                "ShardedModel from database with missing shard_id - this must be a bug")

    @property
    def _db(self):
        return ctx.db.shards[self._shard_id]

    def save(self, skip_callback=False, invalidate_cache=True):
        if self._shard_id is None:
            raise MissingShardId(
                "ShardedModel must have shard_id set before save")
        super().save(skip_callback, invalidate_cache)

    def _refetch_from_db(self):
        return self.find_one(self._shard_id, {"_id": self._id})

    @classmethod
    def _get_possible_databases(cls):
        return list(ctx.db.shards.values())

    @classmethod
    def find(cls, shard_id, query=None, **kwargs):
        if not query:
            query = {}
        return ctx.db.get_shard(shard_id).get_objs(
            cls.from_data,
            cls.collection,
            cls._preprocess_query(query),
            **kwargs
        )

    @classmethod
    def aggregate(cls, shard_id, pipeline, query=None, **kwargs):
        if not query:
            query = {}
        pipeline = [{"$match": cls._preprocess_query(query)}] + pipeline
        return ctx.db.get_shard(shard_id).get_aggregated(cls.collection, pipeline, **kwargs)

    @classmethod
    def find_projected(cls, shard_id, query=None, projection=('_id',), **kwargs):
        if not query:
            query = {}
        return ctx.db.get_shard(shard_id).get_objs_projected(
            cls.collection,
            cls._preprocess_query(query),
            projection=projection,
            **kwargs
        )

    @classmethod
    def find_one(cls, shard_id, query, **kwargs):
        return ctx.db.get_shard(shard_id).get_obj(
            cls.from_data,
            cls.collection,
            cls._preprocess_query(query),
            **kwargs
        )

    @classmethod
    def get(cls, shard_id, expression, raise_if_none=None):
        if expression is None:
            return None
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
        if expression is None:
            return None
        cache_key = f"{cls.collection}.{shard_id}.{expression}"
        getter = partial(cls.get, shard_id, expression, raise_if_none)
        constructor = partial(cls.from_data, shard_id=shard_id)
        obj = cls._cache_get(cache_key, getter, constructor)
        obj.shard_id = shard_id
        return obj

    def invalidate(self, _id=None):
        if _id is None:
            _id = self._id
        cache_key_id = f"{self.collection}.{self._shard_id}.{_id}"
        cache_key_keyfield = None
        if self.KEY_FIELD is not None and self.KEY_FIELD != "_id":
            cache_key_keyfield = f"{self.collection}.{self._shard_id}.{getattr(self, self.KEY_FIELD)}"

        return self._invalidate(cache_key_id, cache_key_keyfield)

    @classmethod
    def destroy_all(cls, shard_id):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).delete_query(
            cls.collection, cls._preprocess_query({}))

    @classmethod
    def destroy_many(cls, shard_id, query):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).delete_query(
            cls.collection, cls._preprocess_query(query))

    @classmethod
    def update_many(cls, shard_id, query, attrs):
        # warning: being a faster method than traditional model manipulation,
        # this method doesn't provide any lifecycle callback for independent
        # objects
        ctx.db.get_shard(shard_id).update_query(
            cls.collection, cls._preprocess_query(query), attrs)
