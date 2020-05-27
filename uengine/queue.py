import redis
import socket
import random
from uengine import ctx
from uengine.utils import now, uuid4_string
from flask import json
from time import time


class Task:

    def __init__(self, task_type, data, created_at=None, task_id=None):
        self.id = task_id or uuid4_string()
        self.type = task_type
        self.data = data
        self.created_at = created_at or now()
        self.received_by = None

    def to_message(self):
        return json.dumps({
            "id": self.id,
            "type": self.type,
            "data": self.data,
            "created_at": self.created_at
        })

    def set_recv_by(self, recv_by):
        self.received_by = recv_by

    @property
    def received(self):
        return self.received_by is not None

    @classmethod
    def from_message(cls, msg):
        if "data" not in msg:
            ctx.log.error("malformed message, no data field: %s", msg)
        msg = msg["data"]
        if isinstance(msg, str) or isinstance(msg, bytes):
            msg = json.loads(msg)
        task_id = msg["id"]
        task_type = msg["type"]
        created_at = msg["created_at"]
        data = msg["data"]
        return cls(task_type=task_type, task_id=task_id, data=data, created_at=created_at)

    def __str__(self):
        return f"<Task {self.type} id={self.id} data={json.dumps(self.data)} created_at={self.created_at} received_by={self.received_by}>"


class RedisQueue:

    ACK_POSTFIX = ":ack"

    def __init__(self, redis_cfg):
        self.cfg = redis_cfg
        self.ack_timeout = redis_cfg.get("ack_timeout", 1)
        self.retries = redis_cfg.get("retries", 3)
        self._conn = None
        self._ackconn = None
        self._ps = None
        self.prefix = self.cfg.get("channel", "ueq")

        fqdn = socket.getfqdn()
        rand = random.randint(0, 10000)
        self.msgchannel = f"{self.prefix}:{fqdn}:{rand}"
        self.ackchannel = f"{self.prefix}:{fqdn}:{rand}:ack"

    def __init_conn(self):
        ctx.log.info("creating a new redis connection")
        host = self.cfg.get("host")
        port = self.cfg.get("port")
        dbname = self.cfg.get("db")
        extra = self.cfg.get("options", {})
        r = redis.Redis(host=host, port=port, db=dbname, **extra)
        return r

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.__init_conn()
        return self._conn

    @property
    def ackconn(self):
        if self._ackconn is None:
            self._ackconn = self.__init_conn()
        return self._ackconn

    @property
    def ps(self):
        if self._ps is None:
            self._ps = self.conn.pubsub(ignore_subscribe_messages=True)
        return self._ps

    def get_random_channel(self):
        channels = [ch.decode() for ch in self.conn.execute_command("PUBSUB channels")]
        channels = [ch for ch in channels
                    if ch.startswith(self.prefix) and
                    not ch.endswith(self.ACK_POSTFIX)]
        rand = random.randrange(0, len(channels))
        return channels[rand], channels[rand] + self.ACK_POSTFIX

    @staticmethod
    def wait_for_msg(pubsub, timeout):
        cancel_at = time() + timeout
        while time() < cancel_at:
            msg = pubsub.get_message()
            if msg:
                return msg
        return None

    def enqueue(self, task):
        if not isinstance(task, Task):
            raise TypeError("only instances of Task are allowed")
        chan, ackchan = self.get_random_channel()

        ack = None
        ackps = self.ackconn.pubsub(ignore_subscribe_messages=True)
        retries = self.retries
        while retries > 0:
            ackps.subscribe(ackchan)
            self.conn.publish(chan, task.to_message())
            ack = self.wait_for_msg(ackps, self.ack_timeout)
            ackps.unsubscribe(ackchan)
            if ack:
                break
            retries -= 1
            ctx.log.debug("error receiving ack for task id %s, resending, %d retries left",
                          task.id, retries)

        if ack:
            recvchan = ack["channel"].decode()
            receiver = recvchan[len(self.prefix)+1:-len(self.ACK_POSTFIX)]
            task.set_recv_by(receiver)

        return ack

    def ack(self, task_id):
        self.conn.publish(self.ackchannel, task_id)

    def subscribe(self):
        return self.ps.subscribe(self.msgchannel)

    def get_message(self, **kwrds):
        return self.ps.get_message(**kwrds)

    @property
    def tasks(self):
        for msg in self.ps.listen():
            try:
                task = Task.from_message(msg)
                self.ack(task.id)
                task.set_recv_by(self.msgchannel[len(self.prefix)+1:])
                yield task
            except Exception as e:
                ctx.log.error("error receiving message: %s", e)
