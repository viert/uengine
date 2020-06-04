from .task import BaseTask
from ..context import ctx
DEFAULT_RETRIES = 5
DEFAULT_ACK_TIMEOUT = 1


class AbstractQueue:

    ACK_POSTFIX = ":ack"

    def __init__(self, qcfg):
        self.cfg = qcfg

    def enqueue(self, task):
        if not isinstance(task, BaseTask):
            raise TypeError("only instances of Task are allowed")
        if task.received:
            raise RuntimeError("task is already received by a subscriber")
        ctx.log.debug("%s: enqueue task %s", self.__class__.__name__, task)
        ack = self._enqueue(task)
        if ack:
            ctx.log.debug("%s: task %s enqueued",
                          self.__class__.__name__, task)

    def _enqueue(self, task):
        raise NotImplementedError("abstract queue")

    def ack(self, task_id):
        raise NotImplementedError("abstract queue")

    def subscribe(self):
        raise NotImplementedError("abstract queue")

    def list_active_channels(self):
        raise NotImplementedError("abstract queue")

    @property
    def tasks(self):
        raise NotImplementedError("abstract queue")
