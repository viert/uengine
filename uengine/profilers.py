import cProfile
import io
import line_profiler
import pstats

from . import ctx
from .api import get_boolean_request_param

from flask import g


def before_request():
    if get_boolean_request_param("profile"):
        if ctx.line_profiler.functions:
            g.line_profiler = line_profiler.LineProfiler(*ctx.line_profiler.functions)
            g.line_profiler.enable()
        g.profiler = cProfile.Profile()
        g.profiler.enable()


def after_request(response):
    if hasattr(g, "profiler"):
        g.profiler.disable()
        strio = io.StringIO()
        ps = pstats.Stats(g.profiler, stream=strio).sort_stats('cumulative')
        ps.print_stats()
        ctx.log.debug(strio.getvalue())

    if hasattr(g, "line_profiler"):
        g.line_profiler.disable()
        strio = io.StringIO()
        g.line_profiler.print_stats(stream=strio)
        ctx.log.debug(strio.getvalue())

    return response
