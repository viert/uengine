from commands import Command
from commands.send import MessageTask
from uengine import ctx


class Tasks(Command):

    def run(self):
        MessageTask.register()
        ctx.log.info("running task list server")
        q = ctx.queue
        q.subscribe()
        for task in q.tasks:
            ctx.log.info("Got task %s", task)
            ctx.log.info("Task message is %s", task.message)
