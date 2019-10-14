"""
>>> import time
>>> import flask
>>> import uuid
>>> from uengine.afterlife import Afterlife, after_this_response, g

>>> app = flask.Flask("after_response")
>>> Afterlife(app)

>>> @app.after_response
>>> def say_oops():
>>>     time.sleep(1)
>>>     print("Oops...")
>>>     raise RuntimeError("Wheee!")

>>> @app.after_response
>>> def say_bye():
>>>     time.sleep(0.1)
>>>     print(f"Bye {g.request_id}")

>>> @app.route("/")
>>> def home():
>>>     g.request_id = str(uuid.uuid4())
>>>
>>>     @after_this_response
>>>     def say_wapapapapapapow():
>>>         time.sleep(1.5)
>>>         print("Wa-pa-pa-pa-pa-pa-pow")
>>>         time.sleep(0.01)
>>>
>>>     after_this_response(lambda: print("What does the fox say?"))
>>>
>>>     return "Success!\n"
"""

import functools

from itertools import chain
from werkzeug.local import LocalStack, LocalProxy
from werkzeug.wsgi import ClosingIterator


class Afterlife:
    def __init__(self, app=None, patch_test_client=True, logger=None):
        """
        :param app: Flask app to apply middleware to
        :param patch_test_client: Make test client buffer responses by default.
                                  It is makes the client close the request before
                                  returning, thus running all afterlife callbacks.
                                  See Werkzeug.test.Client.open() for details.
        :param logger: Logger to use. By default app.logger will be used
        """
        self.after_response_functions = []
        self.patch_test_client = patch_test_client
        self.logger = logger
        if app:
            self.init_app(app)

    def after_response(self, func):
        self.after_response_functions.append(func)
        return func

    def init_app(self, app):
        # install extension
        app.after_response = self.after_response
        # install middleware
        app.wsgi_app = _AfterlifeMiddleware(app.wsgi_app, self.run_after_response, app.logger)
        if not self.logger:
            self.logger = app.logger

        if not self.patch_test_client:
            return

        test_client_orig = app.test_client

        @functools.wraps(app.test_client)
        def patched_test_client(*args, **kwargs):
            client = test_client_orig(*args, **kwargs)
            client.open = functools.partial(client.open, buffered=True)
            return client

        app.test_client = patched_test_client

    def run_after_response(self):
        funcs = chain(_after_response_functions, self.after_response_functions)
        for func in funcs:
            try:
                func()
            except Exception:
                self.logger.exception("Unhandled exception")


def after_this_response(func):
    """
    Schedules a callable to be executed after current request has been responded to
    If called outside of any request executes func immediately synchronously.
    :param func: Callable to schedule after this response
    :return func itself. Can be used as a decorator
    """
    if has_context():
        _after_response_functions.append(func)
    else:
        func()
    return func


class _AfterlifeMiddleware:
    def __init__(self, application, after_response_callback, logger):
        self.application = application
        self.after_response_callback = after_response_callback
        self.logger = logger

    @staticmethod
    def set_local_store():
        _afterlife_ctx_stack.push(AfterlifeContext())

    @staticmethod
    def clear_local_store():
        _afterlife_ctx_stack.pop()

    def _after_response(self):
        try:
            self.after_response_callback()
        finally:
            self.clear_local_store()

    def __call__(self, environ, start_response):
        self.set_local_store()
        iterator = self.application(environ, start_response)
        return ClosingIterator(iterator, [self._after_response])


class AfterlifeContext:

    def __init__(self):
        self.g = _AfterlifeGlobals()
        self.after_response_functions = []


_not_set = object()


class _AfterlifeGlobals:

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

    def pop(self, name, default=_not_set):
        if default is _not_set:
            return self.__dict__.pop(name)
        else:
            return self.__dict__.pop(name, default)

    def setdefault(self, name, default=None):
        return self.__dict__.setdefault(name, default)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)


def _from_ctx(attr):
    top = _afterlife_ctx_stack.top
    if top is None:
        raise RuntimeError("Working outside of afterlife request context")
    return getattr(top, attr)


def has_context():
    return _afterlife_ctx_stack.top is not None


_afterlife_ctx_stack = LocalStack()
_after_response_functions = LocalProxy(lambda: _from_ctx("after_response_functions"))
g = LocalProxy(lambda: _from_ctx("g"))
