from commands import Command
# The Worker class should be a subclass of uengine.tasks.BaseWorker
from {{project_name}}.tasks.worker import Worker


class Tasks(Command):

    def run(self):
        w = Worker()
        w.process_tasks()
