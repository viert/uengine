from .abstract_queue import AbstractQueue


class DummyQueue(AbstractQueue):
    """
    DummyQueue is for testing purposes only
    """

    def __init__(self, qcfg):
        super(DummyQueue, self).__init__(qcfg)
        self.queue = []

    def _enqueue(self, task):
        self.queue.append(task)

    def ack(self, task_id):
        return task_id

    def subscribe(self):
        pass

    def list_active_channels(self):
        return [{"chan": "dummy:local"}]

    @property
    def tasks(self):
        while len(self.queue):
            item = self.queue.pop(0)
            yield item
