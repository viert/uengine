from uuid import uuid4
from datetime import datetime, timedelta
from flask.sessions import SessionInterface, SessionMixin
from random import random
from werkzeug.datastructures import CallbackDict

from . import ctx


class MongoSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, sid=None):
        CallbackDict.__init__(self, initial)
        self.sid = sid
        self.modified = False


class MongoSessionInterface(SessionInterface):
    def __init__(self, collection_name='sessions'):
        self.collection_name = collection_name

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if sid:
            stored_session = ctx.db.meta.get_session(sid, collection=self.collection_name)
            if stored_session:
                if stored_session.get('expiration') > datetime.utcnow():
                    return MongoSession(initial=stored_session['data'], sid=stored_session['sid'])
        else:
            sid = str(uuid4())
        return MongoSession(sid=sid)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)

        if not session:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return

        if session.modified:
            session.permanent = True
            expiration = self.get_expiration_time(app, session)
            if not expiration:
                expiration = datetime.now() + timedelta(hours=1)
            ctx.db.meta.update_session(session.sid, session, expiration, collection=self.collection_name)
            response.set_cookie(app.session_cookie_name, session.sid,
                                expires=self.get_expiration_time(app, session),
                                httponly=True, domain=domain)

        if ctx.cfg.get("session_auto_cleanup", True):
            if random() < ctx.cfg.get("session_auto_cleanup_trigger", 0.05):
                ctx.log.info("Cleaning up sessions")
                ctx.db.meta.cleanup_sessions()
