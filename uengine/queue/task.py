from uengine import ctx
from uengine.utils import uuid4_string, now
from flask import json


class BaseTask:

    TYPE = "BASE"
    TYPE_MAP = {}

    def __init__(self, data, created_at=None, task_id=None):
        self.id = task_id or uuid4_string()
        self.data = data
        self.created_at = created_at or now()
        self.received_by = None

    def to_message(self):
        return {
            "id": self.id,
            "type": self.TYPE,
            "data": json.dumps(self.data),
            "created_at": self.created_at
        }

    def set_recv_by(self, recv_by):
        self.received_by = recv_by

    @property
    def received(self):
        return self.received_by is not None

    @classmethod
    def from_message(cls, msg):
        if "data" not in msg:
            ctx.log.error("malformed message, no data field: %s", msg)
        task_id = msg["id"]
        task_type = msg["type"]
        created_at = msg["created_at"]
        data = json.loads(msg["data"])
        if task_type in cls.TYPE_MAP:
            task_class = cls.TYPE_MAP[task_type]
        else:
            task_class = cls
        return task_class(task_id=task_id, data=data, created_at=created_at)

    @classmethod
    def register(cls):
        BaseTask.TYPE_MAP[cls.TYPE] = cls

    def publish(self):
        return ctx.queue.enqueue(self)

    def __str__(self):
        return f"<{self.__class__.__name__} {self.TYPE} id={self.id} data={json.dumps(self.data)} " + \
               f"created_at={self.created_at} received_by={self.received_by}>"

    def __repr__(self):
        return self.__str__()
