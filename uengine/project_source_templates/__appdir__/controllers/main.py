import flask

from uengine import ctx
from uengine.cache import check_cache
from uengine.api import json_response
from .auth_controller import AuthController


def gen_main_ctrl(app):
    main_ctrl = AuthController("main", __name__, require_auth=True)

    def index():
        routes = []
        for rule in app.flask.url_map.iter_rules():
            routes.append({
                "endpoint": rule.endpoint,
                "route": rule.rule,
                "methods": rule.methods
            })
        return json_response({"routes": routes})

    def app_info():
        results = {
            "app": {
                "name": "ex-ya.ru"
            }
        }
        if hasattr(app, "VERSION"):
            results["app"]["version"] = app.VERSION
        else:
            results["app"]["version"] = "unknown"

        results["mongodb"] = {
            "meta": ctx.db.meta.conn.client.server_info(),
            "shards": {}
        }

        for shard_id, shard in ctx.db.shards.items():
            results["mongodb"]["shards"][shard_id] = shard.conn.client.server_info()

        results["flask_version"] = flask.__version__

        results["cache"] = {
            "type": ctx.cache.__class__.__name__,
            "active": check_cache()
        }

        return json_response({"app_info": results})

    main_ctrl.route("/")(index)
    main_ctrl.route("/app_info")(app_info)

    return main_ctrl