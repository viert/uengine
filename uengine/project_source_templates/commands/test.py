from {{project_name}}.models import User, Token
import logging
import mongomock

from commands import Command, _all_tests
from unittest import main
from uengine import ctx
from {{project_name}} import force_init_app
from {{project_name}} import tests


class Test(Command):

    NO_ARGPARSE = True

    @mongomock.patch()
    def run(self):
        force_init_app()
        ctx.log.setLevel(logging.ERROR)
        argv = ['micro.py test', '--buffer'] + self.raw_args
        test_program = main(argv=argv, module=_all_tests, exit=False)
        if test_program.result.wasSuccessful():
            return 0

        return 1
