import pylint.lint

from commands import Command


class Lint(Command):

    NO_ARGPARSE = True

    def run(self):
        lint_args = [
            "exyaru",
            "commands",
            "uengine",
            "micro",
            "wsgi",
        ]
        lint_args += self.raw_args
        linter = pylint.lint.Run(lint_args, do_exit=False)
        return linter.linter.msg_status
