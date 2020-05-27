import logging


class ContextError(Exception):
    pass


_NOT_SET = object()


class _LineProfilerFuncs:
    """This is pseudo-LineProfiler that collects functions to profile later"""

    def __init__(self):
        self.functions = set()

    def __call__(self, func):
        self.functions.add(func)
        return func


def gen_ctx_prop(name, default=_NOT_SET):
    attr_name = f"_{name}"

    def get_no_default(self):
        try:
            prop = getattr(self, attr_name)
        except AttributeError:
            raise ContextError(f"Attempted to use uninitialised {name}")
        return prop

    def get_default(self):
        return getattr(self, attr_name, default)

    def setter(self, value):
        try:
            getattr(self, attr_name)
        except AttributeError:
            setattr(self, attr_name, value)
            return
        # raise ContextError(f"Attempted to overwrite already initialised {name}")

    def deleter(self):
        delattr(self, attr_name)

    getter = get_no_default if default is _NOT_SET else get_default

    return property(fget=getter, fset=setter, fdel=deleter)


class _Context:
    # Although it is possible to assign generated properties to correct attributes
    # inside of _gen_prop(), doing so here explicitly declares them. It helps
    # linters and IDE auto-completers see the attributes and also:
    # >>> import this
    # Explicit is better than implicit
    envtype = gen_ctx_prop("envtype")
    cfg = gen_ctx_prop("cfg")
    log = gen_ctx_prop("log", default=logging)
    db = gen_ctx_prop("db")
    cache = gen_ctx_prop("cache")
    filecache = gen_ctx_prop("filecache")
    queue = gen_ctx_prop("queue")
    line_profiler = _LineProfilerFuncs()


ctx = _Context()
