from uengine import ctx
from uengine.queue import BaseWorker


class Worker(BaseWorker):

    def run_task(self, task):
        """
        override this method to process your tasks
        """
        if task.TYPE == "EXAMPLE":
            self.rt_example(task)
        else:
            ctx.log.error("task type %s is not supported", task.TYPE)

    @staticmethod
    def rt_example(task):
        ctx.log.debug("successfully processed example task id=%s", task.id)
