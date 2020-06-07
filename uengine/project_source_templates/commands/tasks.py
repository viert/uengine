from commands import Command
from {{project_name}}.tasks.worker import process_tasks


class Tasks(Command):

    def run(self):
        process_tasks()
