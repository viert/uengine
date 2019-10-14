from {{project_name}}.models import User, Token
import logging
import mongomock

from uengine.commands import Command
from unittest import main
from uengine import ctx
from {{project_name}} import force_init_app
from {{project_name}} import tests
{% if auth % }
{% endif % }


class Test(Command):

    NO_ARGPARSE = True

    @mongomock.patch()
    def run(self):
        force_init_app()
        ctx.log.level = logging.ERROR
        ctx.cfg["text_storage"] = {
            "posts": {
                "type": "memory",
                "options": {
                    "instance_id": 1,
                }
            },
            "comments": {
                "type": "memory",
                "options": {
                    "instance_id": 2,
                }
            }
        }
        {% if auth % }
        User._collection = "test_users"  # pylint: disable=protected-access
        Token._collection = "test_tokens"  # pylint: disable=protected-access
        {% endif % }

        argv = ['micro.py test'] + self.raw_args
        test_program = main(argv=argv, module=tests, exit=False)
        if test_program.result.wasSuccessful():
            return 0

        return 1
