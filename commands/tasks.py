from commands import Command
from uengine import ctx


class Tasks(Command):

    def run(self):
        ctx.log.info("running task list server")
        q = ctx.queue
        q.subscribe()
        for task in q.tasks:
            ctx.log.info("Got task %s", task)
