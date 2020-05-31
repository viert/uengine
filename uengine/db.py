import pymongo

from contextlib import contextmanager
from bson.objectid import ObjectId, InvalidId
from time import sleep
from datetime import datetime
from random import randint
from pymongo.errors import ServerSelectionTimeoutError
from uengine.errors import InvalidShardId

from . import ctx

MONGO_RETRIES = 6
MONGO_RETRIES_RO = 6
RETRY_SLEEP = 3  # 3 seconds


class AbortTransaction(Exception):
    pass


def intercept_mongo_errors_rw(func):
    def wrapper(*args, **kwargs):
        if "retries_left" in kwargs:
            retries_left = kwargs["retries_left"]
            del kwargs["retries_left"]
        else:
            retries_left = MONGO_RETRIES

        try:
            result = func(*args, **kwargs)
        except ServerSelectionTimeoutError:
            ctx.log.error(
                "ServerSelectionTimeout in db module for read/write operations")
            retries_left -= 1
            if retries_left == MONGO_RETRIES / 2:
                ctx.log.error(
                    "Mongo connection %d retries passed with no result, "
                    "trying to reinstall connection",
                    MONGO_RETRIES / 2
                )
                db_obj = args[0]
                db_obj.reset_conn()
            if retries_left == 0:
                ctx.log.error(
                    "Mongo connection %d retries more passed with no result, giving up", MONGO_RETRIES / 2)
                raise

            sleep(RETRY_SLEEP)
            kwargs["retries_left"] = retries_left
            return wrapper(*args, **kwargs)

        return result

    return wrapper


def intercept_mongo_errors_ro(func):
    def wrapper(*args, **kwargs):
        if "retries_left" in kwargs:
            retries_left = kwargs["retries_left"]
            del kwargs["retries_left"]
        else:
            retries_left = MONGO_RETRIES

        try:
            result = func(*args, **kwargs)
        except pymongo.errors.ServerSelectionTimeoutError:
            ctx.log.error(
                "ServerSelectionTimeout in db module for read-only operations")
            retries_left -= 1
            if retries_left == MONGO_RETRIES_RO / 2:
                ctx.log.error(
                    "Mongo readonly connection %d retries passed, switching "
                    "readonly operations to read-write socket",
                    MONGO_RETRIES_RO / 2
                )
                db_obj = args[0]
                db_obj._ro_conn = db_obj.conn  # pylint: disable=protected-access
            if retries_left == 0:
                raise

            sleep(RETRY_SLEEP)
            kwargs["retries_left"] = retries_left
            return wrapper(*args, **kwargs)

        return result

    return wrapper


class ObjectsCursor:

    def __init__(self, cursor, obj_class, shard_id=None):
        self.obj_class = obj_class
        self.cursor = cursor
        self._shard_id = shard_id

    def all(self):
        return list(self)

    def limit(self, *args, **kwargs):
        self.cursor.limit(*args, **kwargs)
        return self

    def skip(self, *args, **kwargs):
        self.cursor.skip(*args, **kwargs)
        return self

    def sort(self, *args, **kwargs):
        self.cursor.sort(*args, **kwargs)
        return self

    def __iter__(self):
        for item in self.cursor:
            if self._shard_id:
                item["shard_id"] = self._shard_id
            yield self.obj_class(**item)

    def __getitem__(self, item):
        attrs = self.cursor.__getitem__(item)
        if self._shard_id:
            attrs["shard_id"] = self._shard_id
        return self.obj_class(**attrs)

    def __getattr__(self, item):
        return getattr(self.cursor, item)


def pick_rw_shard_id():
    idx = randint(0, len(ctx.db.rw_shards)-1)
    return ctx.db.rw_shards[idx]


class _DB:

    @contextmanager
    def transaction(self):
        client = self.get_rw_client()
        try:
            self._session = client.start_session()
            self._session.start_transaction()
            yield self._session
            self._session.commit_transaction()
        except Exception as e:
            self._session.abort_transaction()
            if not isinstance(e, AbortTransaction):
                raise
        finally:
            self._session.end_session()
            self._session = None

    def __init__(self, dbconf, shard_id=None):
        self._config = dbconf
        self._rw_client = None
        self._ro_client = None
        self._conn = None
        self._ro_conn = None
        self._shard_id = shard_id
        self._session = None

    def reset_conn(self):
        self._rw_client = None
        self._conn = None

    def reset_ro_conn(self):
        self._ro_client = None
        self._ro_conn = None

    def get_rw_client(self):
        if not self._rw_client:
            client_kwargs = self._config.get("pymongo_extra", {})
            self._rw_client = pymongo.MongoClient(
                self._config["uri"], **client_kwargs)
        return self._rw_client

    def get_ro_client(self):
        if not self._ro_client:
            client_kwargs = self._config.get("pymongo_extra", {})
            if "uri_ro" in self._config:
                self._ro_client = pymongo.MongoClient(
                    self._config["uri_ro"], **client_kwargs)
        return self._ro_client

    def init_ro_conn(self):
        ctx.log.info("Creating a read-only mongo connection")
        database = self._config.get('dbname')
        ro_client = self.get_ro_client()
        if ro_client:
            # AUTHENTICATION
            if 'username' in self._config and 'password' in self._config:
                username = self._config["username"]
                password = self._config['password']
                ro_client[database].authenticate(username, password)
            self._ro_conn = ro_client[database]
        else:
            ctx.log.info(
                "No uri_ro option found in configuration, falling back to read/write default connection")
            self._ro_conn = self.conn

    def init_conn(self):
        ctx.log.info("Creating a read/write mongo connection")
        client = self.get_rw_client()
        database = self._config['dbname']
        # AUTHENTICATION
        if 'username' in self._config and 'password' in self._config:
            username = self._config["username"]
            password = self._config['password']
            client[database].authenticate(username, password)
        self._conn = client[database]

    @property
    def conn(self):
        if self._conn is None:
            self.init_conn()
        return self._conn

    @property
    def ro_conn(self):
        if self._ro_conn is None:
            self.init_ro_conn()
        return self._ro_conn

    @intercept_mongo_errors_ro
    @ctx.line_profiler
    def get_obj(self, cls, collection, query):
        if not isinstance(query, dict):
            try:
                query = {'_id': ObjectId(query)}
            except InvalidId:
                pass
        data = self.ro_conn[collection].find_one(query, session=self._session)
        if data:
            if self._shard_id:
                data["shard_id"] = self._shard_id
            return cls(**data)

        return None

    @intercept_mongo_errors_ro
    def get_obj_id(self, collection, query):
        return self.ro_conn[collection].find_one(query, projection=(), session=self._session)['_id']

    @intercept_mongo_errors_ro
    def get_objs(self, cls, collection, query, **kwargs):
        if self._session:
            kwargs["session"] = self._session
        cursor = self.ro_conn[collection].find(query, **kwargs)
        return ObjectsCursor(cursor, cls, shard_id=self._shard_id)

    @intercept_mongo_errors_ro
    def get_objs_projected(self, collection, query, projection, **kwargs):
        if self._session:
            kwargs["session"] = self._session
        cursor = self.ro_conn[collection].find(
            query, projection=projection, **kwargs)
        return cursor

    @intercept_mongo_errors_ro
    def get_aggregated(self, collection, pipeline, **kwargs):
        if self._session:
            kwargs["session"] = self._session
        cursor = self.ro_conn[collection].aggregate(pipeline, **kwargs)
        return cursor

    @intercept_mongo_errors_ro
    def count_docs(self, collection, query, **kwargs):
        return self.ro_conn[collection].count_documents(query, **kwargs)

    def get_objs_by_field_in(self, cls, collection, field, values, **kwargs):
        return self.get_objs(
            cls,
            collection,
            {
                field: {
                    '$in': values,
                },
            },
            **kwargs
        )

    @intercept_mongo_errors_rw
    def save_obj(self, obj):
        if obj.is_new:
            # object to_dict() method should always return all fields
            data = obj.to_dict(include_restricted=True)
            # although with the new object we shouldn't pass _id=null to mongo
            del data["_id"]
            inserted_id = self.conn[obj.collection].insert_one(
                data, session=self._session).inserted_id
            obj._id = inserted_id
        else:
            self.conn[obj.collection].replace_one(
                {'_id': obj._id}, obj.to_dict(include_restricted=True), upsert=True, session=self._session)

    @intercept_mongo_errors_rw
    def delete_obj(self, obj):
        if obj.is_new:
            return
        self.conn[obj.collection].delete_one(
            {'_id': obj._id}, session=self._session)

    @intercept_mongo_errors_rw
    def find_and_update_obj(self, obj, update, when=None):
        query = {"_id": obj._id}
        if when:
            assert "_id" not in when
            query.update(when)

        new_data = self.conn[obj.collection].find_one_and_update(
            query,
            update,
            return_document=pymongo.ReturnDocument.AFTER,
            session=self._session
        )
        if new_data and self._shard_id:
            new_data["shard_id"] = self._shard_id
        return new_data

    @intercept_mongo_errors_rw
    def delete_query(self, collection, query):
        return self.conn[collection].delete_many(query, session=self._session)

    @intercept_mongo_errors_rw
    def update_query(self, collection, query, update):
        return self.conn[collection].update_many(query, update, session=self._session)

    # SESSIONS

    @intercept_mongo_errors_ro
    def get_session(self, sid, collection='sessions'):
        return self.ro_conn[collection].find_one({'sid': sid})

    @intercept_mongo_errors_rw
    def update_session(self, sid, data, expiration, collection='sessions'):
        self.conn[collection].update(
            {'sid': sid}, {'sid': sid, 'data': data, 'expiration': expiration}, True)

    @intercept_mongo_errors_rw
    def cleanup_sessions(self, collection='sessions'):
        return self.conn[collection].remove({'expiration': {'$lt': datetime.now()}})["n"]


class DB:

    INFO_FIELDS = (
        "allocator",
        "bits",
        "debug",
        "gitVersion",
        "javascriptEngine",
        "maxBsonObjectSize",
        "modules",
        "ok",
        "openssl",
        "storageEngines",
        "sysInfo",
        "version",
        "versionArray"
    )

    def __init__(self):
        self.meta = _DB(ctx.cfg["database"]["meta"])
        self.shards = {}
        if "shards" in ctx.cfg["database"]:
            for shard_id, config in ctx.cfg["database"]["shards"].items():
                self.shards[shard_id] = _DB(config, shard_id)

        if "open_shards" in ctx.cfg["database"]:
            self.rw_shards = ctx.cfg["database"]["open_shards"]
        else:
            self.rw_shards = list(self.shards.keys())

    def get_shard(self, shard_id):
        if shard_id not in self.shards:
            raise InvalidShardId(f"shard {shard_id} doesn't exist")
        return self.shards[shard_id]

    def mongodb_info(self):

        def sys_info(raw_info):
            return {k: v for k, v in raw_info.items() if k in self.INFO_FIELDS}

        return dict(
            meta=sys_info(self.meta.conn.client.server_info()),
            shards={shard_id: sys_info(
                self.shards[shard_id].conn.client.server_info()) for shard_id in self.shards}
        )
