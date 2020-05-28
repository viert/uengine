
class AbstractQueue:

    ACK_POSTFIX = ":ack"

    def __init__(self, qcfg):
        self.cfg = qcfg

    def enqueue(self, task):
        raise NotImplementedError("abstract queue")

    def ack(self, task_id):
        raise NotImplementedError("abstract queue")

    def subscribe(self):
        raise NotImplementedError("abstract queue")

    def get_message(self, **kwrds):
        raise NotImplementedError("abstract queue")

    @property
    def tasks(self):
        raise NotImplementedError("abstract queue")
