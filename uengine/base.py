import os
import sys
import inspect
import logging

from flask import Flask, g, request
from datetime import timedelta
from logging.handlers import WatchedFileHandler
from cachelib import MemcachedCache, SimpleCache
from uuid import uuid4

from . import ctx
from .db import DB
from .errors import handle_api_error, handle_other_errors, ApiError
from .sessions import MongoSessionInterface
from .json_encoder import MongoJSONEncoder
from .file_cache import FileCache
from .queue import RedisQueue, MongoQueue, DummyQueue

ENVIRONMENT_TYPES = ("development", "testing", "production")
DEFAULT_ENVIRONMENT_TYPE = "development"
DEFAULT_SESSION_EXPIRATION_TIME = 86400 * 7 * 2
DEFAULT_TOKEN_TTL = 86400 * 7 * 2
DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(request_id)s %(message)s"
DEFAULT_LOG_LEVEL = "debug"
DEFAULT_FILECACHE_DIR = "/var/cache/uengine"


class RequestIDFilter(logging.Filter):
    def filter(self, record):
        if request:
            if 'X-Request-ID' in request.headers:
                record.request_id = request.headers.get('X-Request-ID')
            elif 'request_id' in request.args:
                record.request_id = request.args.get('request_id')
            else:
                record.request_id = getattr(g, "request_id", None)
        else:
            record.request_id = None

        return True


class Base:

    def __init__(self):
        class_file = inspect.getfile(self.__class__)
        self.app_dir = os.path.dirname(os.path.abspath(class_file))
        self.base_dir = os.path.abspath(os.path.join(self.app_dir, "../"))

        self.version = "development"
        self.__set_version()

        envtype = os.getenv("UENGINE_ENV",
                            os.getenv("MICROENG_ENV",
                                      DEFAULT_ENVIRONMENT_TYPE))
        if envtype not in ENVIRONMENT_TYPES:
            envtype = DEFAULT_ENVIRONMENT_TYPE
        ctx.envtype = envtype

        self.session_expiration_time = None
        self.session_auto_cleanup = None
        self.session_auto_cleanup_trigger = None

        # Later steps depend on the earlier ones. The order is important here
        ctx.cfg = self.__read_config()
        ctx.log = self.__setup_logging()  # Requires ctx.cfg
        ctx.db = DB()  # Requires ctx.cfg
        ctx.queue = self.__setup_queue()
        self.flask = self.__setup_flask()  # requires ctx.cfg and ctx.log
        self.__setup_error_handling()  # requires self.flask
        ctx.cache = self.__setup_cache()  # requires ctx.cfg and ctx.log
        ctx.filecache = self.__setup_filecache()  # requires ctx.cfg and ctx.log
        self.__setup_sessions()
        self.configure_routes()
        self.after_configured()

    def configure_routes(self):
        pass

    def after_configured(self):
        pass

    def __set_version(self):
        ver_filename = os.path.join(self.base_dir, "__version__")
        if not os.path.isfile(ver_filename):
            return
        with open(ver_filename) as verf:
            self.version = verf.read().strip()

    @staticmethod
    def __setup_cache():
        ctx.log.debug("Setting up a cache")
        if "memcache_backends" in ctx.cfg:
            return MemcachedCache(ctx.cfg.get("memcache_backends"))

        from .cache import patch_delete_many
        SimpleCache.delete_many = patch_delete_many
        return SimpleCache()

    @staticmethod
    def __setup_filecache():
        ctx.log.debug("Setting up a filecache")
        filecache_dir = ctx.cfg.get("filecache_dir", DEFAULT_FILECACHE_DIR)
        return FileCache(filecache_dir)

    @staticmethod
    def __setup_queue():
        ctx.log.debug("Setting up a queue")
        qcfg = ctx.cfg.get("queue", {})
        qtype = qcfg.get("type", "mongo")  # default internal naive queue

        if qtype == "redis":
            try:
                q = RedisQueue(qcfg)
                return q
            except Exception as e:
                ctx.log.error("Error configuring redis queue: %s", e)
        elif qtype == "mongo":
            try:
                q = MongoQueue(qcfg)
                return q
            except Exception as e:
                ctx.log.error("Error configuring mongo queue: %s", e)
        elif qtype == "dummy":
            try:
                q = DummyQueue(qcfg)
                return q
            except Exception as e:
                ctx.log.error("Error configuring dummy queue: %s", e)

    @staticmethod
    def __setup_logging():
        logger = logging.getLogger("app")
        logger.propagate = False

        log_level = ctx.cfg.get("log_level", DEFAULT_LOG_LEVEL)
        log_level = log_level.upper()
        log_level = getattr(logging, log_level)

        if "log_file" in ctx.cfg:
            handler = WatchedFileHandler(ctx.cfg.get("log_file"))
            logger.addHandler(handler)

        if ctx.cfg.get("debug") or not logger.handlers:
            handler = logging.StreamHandler(stream=sys.stdout)
            logger.addHandler(handler)

        log_format = ctx.cfg.get("log_format", DEFAULT_LOG_FORMAT)
        log_format = logging.Formatter(log_format)

        logger.setLevel(log_level)
        for handler in logger.handlers:
            handler.setLevel(log_level)
            handler.setFormatter(log_format)

        logger.addFilter(RequestIDFilter())

        logger.info("Logger created, starting up")
        return logger

    @staticmethod
    def __setup_flask():
        ctx.log.info("Environment type is %s", ctx.envtype)
        ctx.log.debug("Setting up Flask")
        flask = Flask(__name__, static_folder=None)

        ctx.log.debug("Applying Flask settings")
        if "flask_settings" in ctx.cfg:
            for k, v in ctx.cfg.get("flask_settings").items():
                flask.config[k] = v
        flask.secret_key = ctx.cfg.get("app_secret_key")
        ctx.log.debug("Setting up JSON encoder")
        flask.json_encoder = MongoJSONEncoder

        # pylint: disable=unused-variable
        @flask.before_request
        def add_request_local_cache():
            g.request_local_cache = {}

        if ctx.cfg.get("debug"):
            ctx.log.info("Setting up request logging due to debug setting")

            try:
                from . import profilers
                flask.before_request(profilers.before_request)
                flask.after_request(profilers.after_request)
            except ImportError:
                ctx.log.error(
                    "error importing profiler, profiling will be disabled")

            # pylint: disable=unused-variable
            @flask.before_request
            def add_request_id():
                setattr(g, "request_id", str(uuid4())[:8])

            @flask.before_request
            def log_all_requests():
                msg = "REQ_%s %s data=%s" % (
                    request.method, request.path, request.json)
                ctx.log.debug(msg)

            @flask.after_request
            def log_all_responses(response):
                if response.content_type == 'application/json':
                    ctx.log.debug(" ".join(map(str, [
                        response.status,
                        str(response.headers).rstrip('\r\n'),
                        response.get_data(),
                    ])))
                else:
                    ctx.log.debug(
                        " ".join(map(str, [response.status, str(response.headers).rstrip('\r\n')])))
                return response

        return flask

    def __read_config(self):
        config_filename = os.path.join(
            self.base_dir, "config", "%s.py" % ctx.envtype)
        with open(config_filename) as f:
            config = {}
            text = f.read()
            code = compile(text, config_filename, 'exec')
            exec(code, config)  # pylint: disable=exec-used
            del config["__builtins__"]
            return config

    def __setup_sessions(self):
        ctx.log.debug("Setting up sessions")

        e_time = ctx.cfg.get("session_ttl", DEFAULT_SESSION_EXPIRATION_TIME)
        self.flask.session_interface = MongoSessionInterface(
            collection_name='sessions')
        self.flask.permanent_session_lifetime = timedelta(seconds=e_time)
        self.session_expiration_time = timedelta(seconds=e_time)
        self.session_auto_cleanup = ctx.cfg.get("session_auto_cleanup", True)
        self.session_auto_cleanup_trigger = ctx.cfg.get(
            "session_auto_cleanup_rand_trigger", 0.05)

    def __setup_error_handling(self):
        self.flask.register_error_handler(ApiError, handle_api_error)
        self.flask.register_error_handler(Exception, handle_other_errors)

    def run(self, **kwargs):
        self.flask.run(**kwargs)
