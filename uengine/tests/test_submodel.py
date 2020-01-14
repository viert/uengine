import itertools

from bson.objectid import ObjectId
from uengine.models.submodel import StorableSubmodel, ShardedSubmodel
from uengine.errors import WrongSubmodel, MissingSubmodel, InputDataError, IntegrityError
from .mongo_mock import MongoMockTest


class _BaseTestSubmodel(MongoMockTest):

    CLASS = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        class TestBaseModel(cls.CLASS):  # pylint: disable=inherit-non-class
            FIELDS = [
                "field1",
                "field2",
            ]
            COLLECTION = "test"

        class Submodel1(TestBaseModel):
            SUBMODEL = "submodel1"

        class Submodel2(TestBaseModel):
            SUBMODEL = "submodel2"

        TestBaseModel.register_submodel(Submodel1.SUBMODEL, Submodel1)
        TestBaseModel.register_submodel(Submodel2.SUBMODEL, Submodel2)

        cls.base_model = TestBaseModel
        cls.submodel1 = Submodel1
        cls.submodel2 = Submodel2

    def test_wrong_input(self):
        with self.assertRaises(WrongSubmodel):
            self.submodel1(_id=ObjectId(), field1="value", submodel="wrong")
        with self.assertRaises(MissingSubmodel):
            self.submodel1(_id=ObjectId(), field1="value")
        with self.assertRaises(InputDataError):
            self.submodel1(field1="value", submodel="my_submodel")
        with self.assertRaises(WrongSubmodel):
            obj = self.submodel1(field1="value")
            obj.submodel = "wrong"
            obj.save()

    def test_submodel_field(self):
        obj = self.submodel1()
        self.assertTrue(hasattr(obj, "submodel"))
        self.assertEqual(obj.submodel, self.submodel1.SUBMODEL)
        obj.save()
        obj.reload()
        self.assertEqual(obj.submodel, self.submodel1.SUBMODEL)
        db_obj = self.submodel1.get(obj._id)
        self.assertEqual(db_obj.submodel, self.submodel1.SUBMODEL)

    def test_inheritance(self):
        class Submodel1(self.base_model):
            SUBMODEL = "submodel1"

        class Submodel1_1(Submodel1):
            pass

        self.assertEqual(self.base_model.collection, Submodel1.collection)
        self.assertEqual(Submodel1.collection, Submodel1_1.collection)
        self.assertEqual(Submodel1.SUBMODEL, Submodel1_1.SUBMODEL)

    def test_abstract(self):
        with self.assertRaises(IntegrityError):
            self.base_model()

        with self.assertRaises(IntegrityError):
            class C(self.base_model):
                pass  # no SUBMODEL
            C()

        with self.assertRaises(IntegrityError):
            class C(self.submodel1):
                SUBMODEL = "c"
            self.submodel1.register_submodel("c", C)

    def _create_objs(self):
        """Returns two lists of objects. Objects in the same positions only differ in their submodel"""
        values = [1, 2, 3]
        objs1 = [self.submodel1(field1=v, field2=v) for v in values]
        objs2 = [self.submodel2(field1=v, field2=v) for v in values]
        for obj in itertools.chain(objs1, objs2):
            obj.save()

        return objs1, objs2

    def test_isolation_find(self):
        objs1, objs2 = self._create_objs()
        self.assertCountEqual(
            self.submodel1.find().all(),
            objs1,
        )
        self.assertCountEqual(
            self.submodel2.find().all(),
            objs2,
        )
        self.assertCountEqual(
            self.base_model.find().all(),
            objs1 + objs2,
        )

        self.assertCountEqual(
            self.submodel1.find({"field1": objs1[0].field1}).all(),
            [objs1[0]],
        )
        self.assertCountEqual(
            self.base_model.find({"field1": objs1[0].field1}).all(),
            [objs1[0], objs2[0]],
        )

    def test_isolation_update(self):
        objs1, objs2 = self._create_objs()
        obj1 = objs2[0]
        obj1.field2 = "new_value"
        self.submodel2.update_many(
            {"field1": obj1.field1},
            {"$set": {"field2": obj1.field2}}
        )
        self.assertCountEqual(
            self.base_model.find({"field2": obj1.field2}),
            [obj1]
        )

        obj1 = objs1[1]
        obj2 = objs2[1]
        obj1.field2 = "newer_value"
        obj2.field2 = "newer_value"
        self.base_model.update_many(
            {"field1": obj1.field1},
            {"$set": {"field2": obj1.field2}}
        )
        self.assertCountEqual(
            self.base_model.find({"field2": obj1.field2}),
            [obj1, obj2]
        )

    def test_isolation_destroy(self):
        objs1, objs2 = self._create_objs()
        to_destroy = objs2.pop()
        to_keep = objs1[-1]
        self.submodel2.destroy_many({"field1": to_destroy.field1})
        self.assertListEqual(
            self.submodel2.find({
                "field1": to_destroy.field1
            }).all(),
            []
        )
        self.assertListEqual(
            self.base_model.find({
                "field1": to_destroy.field1
            }).all(),
            [to_keep]
        )

        to_destroy = objs1[0]
        objs1 = objs1[1:]
        objs2 = objs2[1:]
        self.base_model.destroy_many({"field1": to_destroy.field1})
        self.assertCountEqual(
            self.base_model.find().all(),
            objs1 + objs2,
        )

    def test_double_register(self):
        with self.assertRaises(IntegrityError):
            class NewSubmodel(self.base_model):
                SUBMODEL = self.submodel2.SUBMODEL
            self.base_model.register_submodel(
                NewSubmodel.SUBMODEL, NewSubmodel)


class TestShardedSubmodel(_BaseTestSubmodel):

    # pylint: disable=arguments-differ
    class CLASS(ShardedSubmodel):

        def __init__(self, **data):
            data.setdefault("shard_id", "s1")
            super().__init__(**data)

        @classmethod
        def get(cls, *args, **kwargs):
            args = list(args)
            if len(args) == 1:
                args.insert(0, "s1")
            if not args:
                kwargs.setdefault("shard_id", "s1")
            return super().get(*args, **kwargs)

        @classmethod
        def find(cls, *args, **kwargs):
            args = list(args)
            if len(args) == 1:
                args.insert(0, "s1")
            if not args:
                kwargs.setdefault("shard_id", "s1")
            return super().find(*args, **kwargs)

        @classmethod
        def destroy_all(cls, *args, **kwargs):
            args = list(args)
            if not args:
                kwargs.setdefault("shard_id", "s1")
            return super().destroy_all(*args, **kwargs)

        @classmethod
        def destroy_many(cls, *args, **kwargs):
            args = list(args)
            if len(args) == 1:
                args.insert(0, "s1")
            if not args:
                kwargs.setdefault("shard_id", "s1")
            return super().destroy_many(*args, **kwargs)

        @classmethod
        def update_many(cls, *args, **kwargs):
            args = list(args)
            if len(args) == 2:
                args.insert(0, "s1")
            if not args:
                kwargs.setdefault("shard_id", "s1")
            return super().update_many(*args, **kwargs)


class TestStorableSubmodel(_BaseTestSubmodel):

    CLASS = StorableSubmodel
