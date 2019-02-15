# pylint: disable=protected-access

from uengine import ctx
from uengine.models.sharded_model import ShardedModel, MissingShardId
from .mongo_mock import MongoMockTest

CALLABLE_DEFAULT_VALUE = 4


def callable_default():
    return CALLABLE_DEFAULT_VALUE


class TestModel(ShardedModel):
    FIELDS = (
        '_id',
        'field1',
        'field2',
        'field3',
        'callable_default_field'
    )

    DEFAULTS = {
        'field1': 'default_value',
        'field3': 'required_default_value',
        'callable_default_field': callable_default
    }

    REQUIRED_FIELDS = (
        'field2',
        'field3',
    )

    REJECTED_FIELDS = (
        'field1',
    )

    INDEXES = (
        "field1",
    )


class TestShardedModel(MongoMockTest):

    def setUp(self):
        super().setUp()
        for shard_id in ctx.db.shards:
            TestModel.destroy_all(shard_id)

    def tearDown(self):
        for shard_id in ctx.db.shards:
            TestModel.destroy_all(shard_id)
        super().tearDown()

    def test_init(self):
        model = TestModel(field1="value")
        self.assertEqual(model.field1, "value")
        model._before_delete()
        model._before_save()

    def test_shard(self):
        model = TestModel(field2="value")
        self.assertIsNone(model._shard_id)
        self.assertRaises(MissingShardId, model.save)

        shard_id = ctx.db.rw_shards[0]
        model = TestModel(shard_id=shard_id, field2="value")
        self.assertEqual(model._shard_id, shard_id)
        model.save()
