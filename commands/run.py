from commands import Command
from testapp import app


class Run(Command):
    def run(self):
        app.run(debug=True)