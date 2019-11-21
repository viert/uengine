import os
import pickle
import functools
from hashlib import sha256

from uengine import ctx
from uengine.utils import now
from datetime import timedelta
from .cache import DEFAULT_CACHE_PREFIX, DEFAULT_CACHE_TIMEOUT, _get_cache_key


class FileCache:

    def __init__(self, cache_dir=""):
        self.cache_dir = cache_dir
        if not os.path.isdir(cache_dir):
            try:
                os.makedirs(cache_dir, 0o755)
            except (OSError, IOError) as e:
                ctx.log.error(
                    "error creating cache directory %s: %s", cache_dir, e)
                self.initialized = False
                return
        self.initialized = True

    def __getpath(self, key):
        hashed_key = sha256(key.encode())
        return os.path.join(self.cache_dir, hashed_key.hexdigest())

    def set(self, key, value, timeout=None):
        if not self.initialized:
            return False
        path = self.__getpath(key)
        try:
            with open(path, "wb") as cf:
                expires = None
                if timeout is not None:
                    td = timedelta(seconds=timeout)
                    expires = now() + td
                data = {
                    "value": value,
                    "expires": expires
                }
                pickle.dump(data, cf)
        except Exception as e:
            ctx.log.error("error writing filecache %s: %s", path, e)
            return False
        return True

    def __load(self, key):
        if not self.initialized:
            return None
        path = self.__getpath(key)
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "rb") as cf:
                data = pickle.load(cf)
                return data
        except Exception as e:
            ctx.log.error("error loading filecache %s: %s", path, e)
            return None

    def get(self, key):
        data = self.__load(key)
        if data is None:
            return None
        if data["expires"] is not None and now() > data["expires"]:
            self.delete(key)
            return None
        return data["value"]

    def has(self, key):
        data = self.__load(key)
        if data is None:
            return False
        if data["expires"] is not None and now() > data["expires"]:
            self.delete(key)
            return False
        return True

    def expires(self, key):
        data = self.__load(key)
        if data is None:
            return None
        return data["expires"]

    def delete(self, key):
        if not self.initialized:
            return False
        path = self.__getpath(key)
        if os.path.isfile(path):
            try:
                os.unlink(path)
                return True
            except Exception as e:
                ctx.log.error("error deleting filecache %s: %s", path, e)
        return False


def file_cached_function(cache_key_prefix=DEFAULT_CACHE_PREFIX, cache_timeout=DEFAULT_CACHE_TIMEOUT, positive_only=False):
    def cache_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_key, _ = _get_cache_key(
                cache_key_prefix, func.__name__, args, kwargs)
            t1 = now()
            if ctx.filecache.has(cache_key):
                value = ctx.filecache.get(cache_key)
                ctx.log.debug("FileCache HIT %s (%.3f seconds)",
                              cache_key, (now() - t1).total_seconds())
            else:
                value = func(*args, **kwargs)
                if value or not positive_only:
                    ctx.filecache.set(cache_key, value, timeout=cache_timeout)
                ctx.log.debug("FileCache MISS %s (%.3f seconds)",
                              cache_key, (now() - t1).total_seconds())
            return value
        return wrapper
    return cache_decorator
