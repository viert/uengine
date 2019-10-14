from commands import Command
from {{project_name}} import app


class Run(Command):
    def run(self):
        app.run(debug=True)
