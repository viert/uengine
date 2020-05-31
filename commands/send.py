from commands import Command
from uengine import ctx
from uengine.queue import BaseTask
from sys import stdin


class MessageTask(BaseTask):
    TYPE = "MESSAGE"

    @property
    def message(self):
        return self.data["message"]

    @classmethod
    def create(cls, message):
        return cls(data={"message": message})


class Send(Command):

    def run(self):
        ctx.log.info("starting up task client")
        print("Type in messages and send them with Enter:")
        MessageTask.register()

        while True:
            message = stdin.readline().strip()
            if not message:
                break
            t = MessageTask.create(message)
            ctx.log.info("sending task %s", t)
            ctx.queue.enqueue(t)
            ctx.log.info("sent task %s", t)

        ctx.log.info("shutting down task client")