# pylint: disable=protected-access

from uengine import ctx
from uengine.models.storable_model import StorableModel
from .mongo_mock import MongoMockTest

CALLABLE_DEFAULT_VALUE = 4


def callable_default():
    return CALLABLE_DEFAULT_VALUE


class TestModel(StorableModel):

    FIELDS = (
        '_id',
        'field1',
        'field2',
        'field3',
        'callable_default_field'
    )

    VALIDATION_TYPES = {
        "field1": str
    }

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


class TestStorableModel(MongoMockTest):

    def setUp(self):
        super().setUp()
        TestModel.destroy_all()

    def tearDown(self):
        TestModel.destroy_all()
        super().tearDown()

    def test_eq(self):
        model = TestModel(field2="mymodel")
        model.save()
        model2 = TestModel.find_one({"field2": "mymodel"})
        self.assertEqual(model, model2)

    def test_reject_on_update(self):
        model = TestModel(field1="original_value",
                          field2="mymodel_reject_test")
        model.save()
        id_ = model._id
        model.update({"field1": "new_value"})
        model = TestModel.find_one({"_id": id_})
        self.assertEqual(model.field1, "original_value")

    def test_update(self):
        model = TestModel(field1="original_value",
                          field2="mymodel_update_test")
        model.save()
        id_ = model._id
        model.update({"field2": "mymodel_updated"})
        model = TestModel.find_one({"_id": id_})
        self.assertEqual(model.field2, "mymodel_updated")

    def test_update_many(self):
        model1 = TestModel(field1="original_value",
                           field2="mymodel_update_test")
        model1.save()
        model2 = TestModel(field1="original_value",
                           field2="mymodel_update_test")
        model2.save()
        model3 = TestModel(field1="do_not_modify",
                           field2="mymodel_update_test")
        model3.save()

        TestModel.update_many({"field1": "original_value"}, {
                              "$set": {"field2": "mymodel_updated"}})
        model1.reload()
        model2.reload()
        model3.reload()

        self.assertEqual(model1.field2, "mymodel_updated")
        self.assertEqual(model2.field2, "mymodel_updated")
        self.assertEqual(model3.field2, "mymodel_update_test")

    def test_invalidate(self):
        model1 = TestModel(field1="f1", field2="f2", field3="f3")
        model1.save()
        self.assertFalse(ctx.cache.has(f"test_model.{model1._id}"))

        m = model1.cache_get(model1._id)
        self.assertIsNotNone(m)
        self.assertTrue(ctx.cache.has(f"test_model.{model1._id}"))

        model1.save()
        self.assertFalse(ctx.cache.has(f"test_model.{model1._id}"))

        m = model1.cache_get(model1._id)
        self.assertIsNotNone(m)
        self.assertTrue(ctx.cache.has(f"test_model.{model1._id}"))

        m.destroy()
        self.assertFalse(ctx.cache.has(f"test_model.{model1._id}"))
