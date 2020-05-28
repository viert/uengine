import socket
import random
from time import time, sleep
from rediscluster import RedisCluster
from redis import Redis

from .abstract_queue import AbstractQueue
from .task import BaseTask
from uengine import ctx


class AbstractRedisQueue(AbstractQueue):

    def __init__(self, qcfg):
        super(AbstractRedisQueue, self).__init__(qcfg)
        self.ack_timeout = qcfg.get("ack_timeout", 1)
        self.retries = qcfg.get("retries", 3)

        # client instances
        self._conn = None
        self._ackconn = None
        self._ps = None
        self.prefix = self.cfg.get("channel", "ueq")

        fqdn = socket.getfqdn()
        rand = random.randint(0, 10000)
        self.msgchannel = f"{self.prefix}:{fqdn}:{rand}"
        self.ackchannel = f"{self.prefix}:{fqdn}:{rand}:ack"

    def init_conn(self):
        raise NotImplementedError("abstract queue")

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.init_conn()
        return self._conn

    @property
    def ackconn(self):
        if self._ackconn is None:
            self._ackconn = self.init_conn()
        return self._ackconn

    @property
    def ps(self):
        if self._ps is None:
            self._ps = self.conn.pubsub(ignore_subscribe_messages=True)
        return self._ps

    def get_random_channel(self):
        raise NotImplementedError("abstract queue")

    @staticmethod
    def wait_for_msg(pubsub, timeout):
        cancel_at = time() + timeout
        while time() < cancel_at:
            msg = pubsub.get_message()
            if msg:
                return msg
            sleep(.01)
        return None

    def enqueue(self, task):
        if not isinstance(task, BaseTask):
            raise TypeError("only instances of Task are allowed")

        ack = None
        ackps = self.ackconn.pubsub(ignore_subscribe_messages=True)
        retries = self.retries
        while retries > 0:
            chan, ackchan = self.get_random_channel()
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
                task = BaseTask.from_message(msg)
                self.ack(task.id)
                task.set_recv_by(self.msgchannel[len(self.prefix)+1:])
                yield task
            except Exception as e:
                ctx.log.error("error receiving message: %s", e)


class RedisQueue(AbstractRedisQueue):

    def init_conn(self):
        ctx.log.info("creating a new redis connection")
        host = self.cfg.get("host", "127.0.0.1")
        port = self.cfg.get("port", 6379)
        db = self.cfg.get("dbname", 0)
        password = self.cfg.get("password")
        r = Redis(host=host, port=port, db=db, password=password)
        return r

    def get_random_channel(self):
        # the only way to get all the channels
        # active on server from redis client
        channels = [ch.decode() for ch in self.conn.execute_command("PUBSUB channels")]
        channels = [ch for ch in channels
                    if ch.startswith(self.prefix) and
                    not ch.endswith(self.ACK_POSTFIX)]
        rand = random.randrange(0, len(channels))
        return channels[rand], channels[rand] + self.ACK_POSTFIX


class RedisClusterQueue(AbstractRedisQueue):

    def init_conn(self):
        ctx.log.info("creating a new redis connection")
        nodes = self.cfg.get("nodes")
        password = self.cfg.get("password")
        r = RedisCluster(startup_nodes=nodes, password=password)
        return r

    def get_random_channel(self):
        # pubsub_channels() is a cluster-wide method, merging
        # all the channels from different nodes
        channels = [ch.decode() for ch in self.conn.pubsub_channels()]
        channels = [ch for ch in channels
                    if ch.startswith(self.prefix) and
                    not ch.endswith(self.ACK_POSTFIX)]
        rand = random.randrange(0, len(channels))
        return channels[rand], channels[rand] + self.ACK_POSTFIX
