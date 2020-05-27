from commands import Command
from uengine import ctx
from uengine.queue import Task
from sys import stdin


class Send(Command):

    def run(self):
        ctx.log.info("starting up task client")
        print("Type in messages and send them with Enter:")

        while True:
            message = stdin.readline().strip()
            if not message:
                break
            t = Task("MESSAGE", {"message": message})
            ctx.log.info("sending task %s", t)
            ctx.queue.enqueue(t)
            ctx.log.info("sent task %s", t)

        ctx.log.info("shutting down task client")