from commands import Command
from commands.send import MessageTask, BaseTask
from uengine import ctx


class Tasks(Command):

    def run(self):
        BaseTask.register(MessageTask)
        ctx.log.info("running task list server")
        q = ctx.queue
        q.subscribe()
        for task in q.tasks:
            ctx.log.info("Got task %s", task)
