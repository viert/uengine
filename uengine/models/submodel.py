"""
- Make the base class first. Set COLLECTION explicitly - it is not
  generated automatically from the class name but inherited instead.
- Subclass your base class to define submodels. Set SUBMODEL to a string
  that will identify your submodel in the DB.
- You can further subclass your submodels. To avoid saving such abstract
  intermediate models do not set SUBMODEL.
- Register the submodel with the base model

It is possible to register an arbitrary function instead of a proper class.
It may be particularly useful if the correct class depends on something
other than `submodel` field. The function will get **data from the DB and
should return an model object.

If you decide to do it you will likely have to override _preprocess_query()
on your submodels to keep the expected find/destroy/update behaviour

>>> from uengine.models.submodel import StorableSubmodel

>>> class Animal(StorableSubmodel):
>>>     COLLECTION = "animal"
>>>     FIELDS = [
>>>         "weight",
>>>     ]

>>> class Fish(Animal):
>>>     SUBMODEL = "fish"
>>>     FIELDS = [
>>>         "scale_type",
>>>     ]

>>> class Mammal(Animal):
>>>     FIELDS = [
>>>         "penis_len",
>>>     ]

>>> class AquaticMammal(Mammal):
>>>     SUBMODEL = "aquatic_mammal"
>>>     FIELDS = [
>>>         "num_fins",
>>>     ]

>>>> class TerrestrialMammal(Mammal):
>>>     SUBMODEL = "terrestrial_mammal"
>>>     FIELDS = [
>>>         "num_legs",
>>>     ]

>>> Animal.register_submodel(Fish.SUBMODEL, Fish)
>>> Animal.register_submodel(TerrestrialMammal.SUBMODEL, TerrestrialMammal)
>>> Animal.register_submodel(AquaticMammal.SUBMODEL, AquaticMammal)
"""

from .abstract_model import ModelMeta
from .storable_model import StorableModel
from .sharded_model import ShardedModel
from uengine.errors import MissingSubmodel, UnknownSubmodel, WrongSubmodel, InputDataError, IntegrityError


class SubmodelMeta(ModelMeta):

    @staticmethod
    def _get_collection(model_cls, name, bases, dct):
        # SubModels inherit their collections from parent classes.
        # Either set explicitly or inherited
        if hasattr(model_cls, "COLLECTION") and model_cls.COLLECTION:
            return model_cls.COLLECTION

        return None  # No autogeneration


class BaseSubmodelMixin:
    """Do not use this mixin directly. Subclass StorableSubmodel or
    ShardedSubmodel instead"""

    SUBMODEL = None
    AUXILIARY_SLOTS = [
        "SUBMODEL",
    ]
    FIELDS = [
        "submodel",
    ]
    REQUIRED_FIELDS = [
        "submodel",
    ]
    __submodel_loaders = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.is_new:
            if not self.SUBMODEL:
                raise IntegrityError(
                    f"Attempted to create an object of abstract model {self.__class__.__name__}")
            if "submodel" in data:
                raise InputDataError(
                    "Attempt to override submodel for a new object")
            self.submodel = self.SUBMODEL
        else:
            if not self.submodel:
                raise MissingSubmodel(
                    f"{self.__class__.__name__} has no submodel in the DB. Bug?")
            self._check_submodel()

    def _check_submodel(self):
        if self.submodel != self.SUBMODEL:
            raise WrongSubmodel(
                f"Attempted to load {self.submodel} as {self.__class__.__name__}. Correct submodel "
                f"would be {self.SUBMODEL}. Bug?"
            )

    def _validate(self):
        super()._validate()
        self._check_submodel()

    @classmethod
    def register_submodel(cls, name, constructor):
        if cls.SUBMODEL:
            raise IntegrityError(
                "Attempted to register a submodel with another submodel")
        if not cls.__submodel_loaders:
            cls.__submodel_loaders = {}
        if name in cls.__submodel_loaders:
            raise IntegrityError(f"Submodel {name} is already registered")
        cls.__submodel_loaders[name] = constructor

    @classmethod
    def from_data(cls, **data):
        if "submodel" not in data:
            raise MissingSubmodel(
                f"{cls.__name__} has no submodel in the DB. Bug?")
        if not cls.__submodel_loaders:
            return cls(**data)
        submodel_name = data["submodel"]
        if submodel_name not in cls.__submodel_loaders:
            raise UnknownSubmodel(
                f"Submodel {submodel_name} is not registered with {cls.__name__}")
        return cls.__submodel_loaders[submodel_name](**data)

    @classmethod
    def _preprocess_query(cls, query):
        if not cls.SUBMODEL:
            return query
        processed = {"submodel": cls.SUBMODEL}
        processed.update(query)
        return processed


class StorableSubmodel(BaseSubmodelMixin, StorableModel, metaclass=SubmodelMeta):
    pass


class ShardedSubmodel(BaseSubmodelMixin, ShardedModel, metaclass=SubmodelMeta):
    pass
