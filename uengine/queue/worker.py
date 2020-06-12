from threading import Thread
from queue import Queue, Empty
from uengine import ctx
from time import sleep


class BaseWorker(Thread):

    def __init__(self, empty_queue_sleep=0.1, task_retries=5, between_retries_sleep=5):
        super(BaseWorker, self).__init__()
        self.stopped = False
        self.eq_sleep = empty_queue_sleep
        self.retries = task_retries
        self.br_sleep = between_retries_sleep

    def run_task(self, task):
        """
        override this method
        """
        raise NotImplementedError("abstract worker")

    def run(self):
        ctx.log.debug("task-processing worker started")
        while not self.stopped:
            try:
                task = self.q.get(False)
                ctx.log.debug("Got task %s", task)
            except Empty:
                sleep(self.eq_sleep)
                continue
            task_retries = self.retries
            done = False
            while task_retries > 0:
                try:
                    self.run_task(task)
                    done = True
                    break
                except Exception as e:
                    ctx.log.error("error executing task %s: %s", task.id, e)
                    task_retries -= 1
                    if task_retries > 0:
                        ctx.log.info("about to restart task %s in %d seconds, retries left %d",
                                     task.id, self.br_sleep, task_retries)
                    sleep(self.br_sleep)
            if not done:
                ctx.log.error("task %s failed, giving up", task.id)

    def process_tasks(self):
        self.q = Queue()
        self.start()

        try:
            while True:
                try:
                    for task in ctx.queue.tasks:
                        self.q.put(task)
                except NotImplementedError:
                    ctx.log.error("you are probably using BaseWorker itself while it's an abstract class")
                    break
                except Exception as e:
                    ctx.log.error("error moving task from task queue to worker: %s", e)
        except KeyboardInterrupt:
            self.stopped = True
            self.join()
