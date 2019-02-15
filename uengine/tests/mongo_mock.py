import mongomock

from unittest import TestCase
from uengine import ctx
from uengine.db import DB


class MongoMockTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        try:
            del ctx.cfg
        except AttributeError:
            pass

        ctx.cfg = {
            "database": {
                "meta": {
                    "uri": "mongodb://zwfbpggeih",
                    "dbname": "unittest_meta",
                },
                "shards": {
                    "s1": {
                        "uri": "mongodb://zwfbpggeih",
                        "dbname": "unittest_s1",
                    },
                    "s2": {
                        "uri": "mongodb://zwfbpggeih",
                        "dbname": "unittest_s2",
                    },
                }
            }
        }

    def setUp(self) -> None:
        super().setUp()
        self.mongo_patcher = mongomock.patch(servers=[("zwfbpggeih")])
        self.mongo_patcher.start()
        try:
            del ctx.db
        except AttributeError:
            pass
        ctx.db = DB()

    def tearDown(self) -> None:
        try:
            del ctx.db
        except AttributeError:
            pass
        self.mongo_patcher.stop()
        super().tearDown()
