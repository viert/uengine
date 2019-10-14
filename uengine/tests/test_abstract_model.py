# pylint: disable=protected-access

from uengine.models.abstract_model import AbstractModel, FieldRequired, InvalidFieldType
from unittest import TestCase

CALLABLE_DEFAULT_VALUE = 4


def callable_default():
    return CALLABLE_DEFAULT_VALUE


class TestModel(AbstractModel):

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

    AUTO_TRIM_FIELDS = ['field1']


class TestAbstractModel(TestCase):

    def test_init(self):
        model = TestModel(field1='value')
        self.assertEqual(model.field1, 'value')
        model._before_delete()
        model._before_save()

    def test_incomplete(self):
        model = TestModel(field1='value')
        self.assertRaises(FieldRequired, model.save)

    def test_incorrect_index(self):
        with self.assertRaises(TypeError):
            class IncorrectIndexModel(AbstractModel):  # pylint: disable=unused-variable
                INDEXES = (
                    "field1"  # No comma - not a tuple
                )

    def test_invalid_type(self):
        model = TestModel(field1=15, field2="any_value")
        self.assertRaises(InvalidFieldType, model.save)

    def test_merge_on_inheritance(self):
        class Parent(AbstractModel):
            FIELDS = ["pfield"]
            VALIDATION_TYPES = {"pfield": str}
            DEFAULTS = {"pfield": "value"}
            REQUIRED_FIELDS = ["pfield"]
            REJECTED_FIELDS = ["pfield"]
            RESTRICTED_FIELDS = ["pfield"]
            INDEXES = ["pfield"]

        class Child(Parent):
            FIELDS = ["cfield"]
            VALIDATION_TYPES = {"cfield": str}
            DEFAULTS = {"cfield": "value"}
            REQUIRED_FIELDS = ["cfield"]
            REJECTED_FIELDS = ["cfield"]
            RESTRICTED_FIELDS = ["cfield"]
            INDEXES = ["cfield"]

        expected_fields = {"pfield", "cfield"}
        self.assertSetEqual(Child.FIELDS, expected_fields | {"_id"})  # _id comes from AbstractModel
        self.assertSetEqual(Child.REQUIRED_FIELDS, expected_fields)
        self.assertSetEqual(Child.REJECTED_FIELDS, expected_fields)
        self.assertSequenceEqual(sorted(Child.INDEXES), sorted(expected_fields))

        # Unrelated Mixins should also work
        class Mixin:  # pylint: disable=unused-variable
            pass

        class ChildNoOverrides(Mixin, Parent):  # pylint: disable=unused-variable
            pass

    def test_collection_inheritance(self):
        class SemiAbstractModel(AbstractModel):
            pass

        class BaseModel(SemiAbstractModel):
            COLLECTION = "my_collection"

        class Model1(BaseModel):
            pass

        self.assertEqual(SemiAbstractModel.collection, "semi_abstract_model")
        self.assertEqual(BaseModel.collection, "my_collection")
        self.assertEqual(Model1.collection, "model1")

    def test_auto_trim(self):
        t = TestModel(field1="   a   \t", field2="b", field3="c")
        t.save()
        self.assertEqual(t.field1, "a")
