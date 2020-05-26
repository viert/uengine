import functools

from datetime import datetime
from hashlib import md5
from flask import g, has_app_context
from random import randint
from . import ctx
from .db import ObjectsCursor


DEFAULT_CACHE_PREFIX = 'uengine'
DEFAULT_CACHE_TIMEOUT = 3600


def req_cache_get(key):
    if not has_app_context():
        return None
    return g.request_local_cache.get(key)


def req_cache_set(key, value):
    if not has_app_context():
        return False
    g.request_local_cache[key] = value
    return True


def req_cache_delete(key):
    if not has_app_context():
        return False
    if key in g.request_local_cache:
        del g.request_local_cache[key]
        return True
    return False


def req_cache_has_key(key):
    if not has_app_context():
        return False
    return key in g.request_local_cache


def _get_cache_key(pref, funcname, args, kwargs):
    key = "%s:%s(%s.%s)" % (
        pref,
        funcname,
        md5(str(args).encode("utf-8")).hexdigest(),
        md5(str(kwargs).encode("utf-8")).hexdigest()
    )

    kwargs_str = ", ".join(["%s=%s" % (x[0], x[1]) for x in kwargs.items()])
    arguments = ""
    if args:
        arguments = ", ".join([str(x) for x in args])
        if kwargs:
            arguments += ", " + kwargs_str
    else:
        if kwargs:
            arguments = kwargs
    cached_call = "%s:%s(%s)" % (pref, funcname, arguments)
    return key, cached_call


def cached_function(cache_key_prefix=DEFAULT_CACHE_PREFIX, cache_timeout=DEFAULT_CACHE_TIMEOUT, positive_only=False):
    def cache_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_key, _ = _get_cache_key(
                cache_key_prefix, func.__name__, args, kwargs)
            t1 = datetime.now()

            if ctx.cache.has(cache_key):
                value = ctx.cache.get(cache_key)
                ctx.log.debug("Cache HIT %s (%.3f seconds)",
                              cache_key, (datetime.now() - t1).total_seconds())
            else:
                value = func(*args, **kwargs)
                if value or not positive_only:
                    ctx.cache.set(cache_key, value, timeout=cache_timeout)
                ctx.log.debug("Cache MISS %s (%.3f seconds)",
                              cache_key, (datetime.now() - t1).total_seconds())
            return value
        return wrapper
    return cache_decorator


def cached_method(prefix, key_field=None, cache_timeout=None, positive_only=False):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            t1 = datetime.now()
            obj = args[0]
            if key_field is not None:
                if not hasattr(obj, key_field):
                    ctx.log.error(
                        f"MethodCache ERROR {obj.__class__.__name__} doesn't have attr {key_field}")
                    return func(*args, **kwargs)
                try:
                    key = str(getattr(obj, key_field))
                except TypeError as e:
                    ctx.log.error(
                        f"MethodCache ERROR parsing {key_field}: {e}")
                    return func(*args, **kwargs)

                cache_key = f"{prefix}.{key}"
            else:
                cache_key = prefix

            if ctx.cache.has(cache_key):
                value = ctx.cache.get(cache_key)
                ctx.log.debug("MethodCache HIT %s (%.3f seconds)",
                              cache_key, (datetime.now() - t1).total_seconds())
            else:
                value = func(*args, **kwargs)
                if value or not positive_only:
                    ctx.cache.set(cache_key, value, timeout=cache_timeout)
                ctx.log.debug("MethodCache MISS %s (%.3f seconds)",
                              cache_key, (datetime.now() - t1).total_seconds())

            return value

        return wrapper
    return decorator


def check_cache():
    k = md5(str(randint(0, 1000000)).encode('utf-8')).hexdigest()
    v = md5(str(randint(0, 1000000)).encode('utf-8')).hexdigest()
    ctx.cache.set(k, v)
    if ctx.cache.get(k) != v:
        return False

    ctx.cache.delete(k)
    return True


def once_per_request(cache_key_prefix=DEFAULT_CACHE_PREFIX + ".once"):
    """
    Decorator used for ensuring a subroutine runs exactly one time per api request.

    This one acts exactly like request_time_cache without returning value and checking if
    it's positive and things like that.
    """
    def cache_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not has_app_context():
                func(*args, **kwargs)
                return
            flag_key, _ = _get_cache_key(
                cache_key_prefix, func.__name__, args, kwargs)
            t1 = datetime.now()

            if not req_cache_has_key(flag_key):
                func(*args, **kwargs)
                req_cache_set(flag_key, True)
                ts = (datetime.now() - t1).total_seconds()
                ctx.log.debug("OncePerRequest MISS %s(%s) (%.3f secs)", func.__name__,
                              flag_key, ts)
            else:
                ts = (datetime.now() - t1).total_seconds()
                ctx.log.debug("OncePerRequest HIT  %s(%s) (%.3f secs)", func.__name__,
                              flag_key, ts)
        return wrapper
    return cache_decorator


def request_time_cache(cache_key_prefix=DEFAULT_CACHE_PREFIX):
    """
    Decorator used for caching data during one api request.
    It's useful while some "list something" handlers with a number of cross-references generate
    many repeating database requests which are known to generate the same response during the api request.
    I.e. list of 20 hosts included in the same group and inheriting the same set of tags/custom fields
    may produce 20 additional db requests and 20 requests for each parent group recursively. This may be fixed
    by caching db responses in flask "g" store.
    """
    def cache_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not has_app_context():
                return func(*args, **kwargs)

            cache_key, _ = _get_cache_key(
                cache_key_prefix, func.__name__, args, kwargs)
            t1 = datetime.now()

            if not req_cache_has_key(cache_key):
                value = func(*args, **kwargs)
                req_cache_set(cache_key, value)
                ts = (datetime.now() - t1).total_seconds()
                ctx.log.debug("RTCache MISS %s(%s) (%.3f secs)",
                              func.__name__, cache_key, ts)
            else:
                value = req_cache_get(cache_key)
                if isinstance(value, ObjectsCursor):
                    value.cursor.rewind()
                ts = (datetime.now() - t1).total_seconds()
                ctx.log.debug("RTCache HIT  %s(%s) (%.3f secs)",
                              func.__name__, cache_key, ts)
            return value
        return wrapper
    return cache_decorator
