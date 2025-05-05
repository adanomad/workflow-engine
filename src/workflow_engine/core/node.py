# workflow_engine/core/node.py
import re
from collections.abc import Mapping
from typing import (
    Any,
    Generic,
    Type,
    TypeVar,
    Unpack,
    _LiteralGenericAlias,  # type: ignore
)

from overrides import final
from pydantic import BaseModel, ConfigDict, model_validator
from pydantic._internal._model_construction import ModelMetaclass

from .context import Context
from .data import Data, Input_contra, Output_co


def get_fields(cls: type[BaseModel]) -> Mapping[str, tuple[type[Any], bool]]:
    fields: Mapping[str, tuple[type[Any], bool]] = {}
    for k, v in cls.model_fields.items():
        assert v.annotation is not None
        fields[k] = (v.annotation, v.is_required())
    return fields


class Params(Data):
    model_config = ConfigDict(
        extra="allow",
        frozen=True,
    )

    # The base class has extra="allow", so that it can be deserialized into any
    # of its subclasses. However, subclasses should set extra="forbid" to block
    # extra fields.
    def __init_subclass__(cls, **kwargs):
        cls.model_config["extra"] = "forbid"
        super().__init_subclass__(**kwargs)


Params_co = TypeVar("Params_co", bound=Params, covariant=True)
T = TypeVar("T")


@final
class Empty(Params):
    """
    A Data and Params class that is explicitly not allowed to have any
    parameters.
    """

    pass


generic_pattern = re.compile(r"^[a-zA-Z]\w+\[.*\]$")


class Node(BaseModel, Generic[Input_contra, Output_co, Params_co]):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    type: str  # should be a literal string for concrete subclasses
    id: str
    # contains any extra fields for configuring the node
    params: Params_co = Empty()  # type: ignore

    @property
    def input_type(self) -> Type[Input_contra]:  # type: ignore (contravariant return type)
        # return Empty to spare users from having to specify the input type on
        # nodes that don't have any input fields
        return Empty  # type: ignore

    @property
    # @abstractmethod
    def output_type(self) -> Type[Output_co]:
        # return Empty to spare users from having to specify the output type on
        # nodes that don't have any output fields
        return Empty  # type: ignore

    @property
    def input_fields(self) -> Mapping[str, tuple[Type[Any], bool]]:
        return get_fields(self.input_type)

    @property
    def output_fields(self) -> Mapping[str, tuple[Type[Any], bool]]:
        return get_fields(self.output_type)

    def __init_subclass__(cls, **kwargs: Unpack[ConfigDict]):
        super().__init_subclass__(**kwargs)  # type: ignore

        while generic_pattern.match(cls.__name__) is not None:
            assert cls.__base__ is not None
            cls = cls.__base__
        name = cls.__name__
        assert name.endswith("Node"), name
        type_annotation = cls.__annotations__.get("type", None)
        if type_annotation is None or not isinstance(
            type_annotation, _LiteralGenericAlias
        ):
            _registry.register_base(cls)
        else:
            (type_name,) = type_annotation.__args__
            assert isinstance(type_name, str), type_name
            assert type_name == name.removesuffix("Node")
            _registry.register(type_name, cls)

    @model_validator(mode="after")  # type: ignore
    def _to_subclass(self):
        """
        Replaces the Node object with an instance of the registered subclass.
        """
        # HACK: This trick only works if the base class can be instantiated, so
        # we cannot make it an ABC even if it has unimplemented methods.
        if _registry.is_base_class(self.__class__):
            cls = _registry.get(self.type)
            if cls is None:
                raise ValueError(f'Node type "{self.type}" is not registered')
            return cls.model_validate(self.model_dump())
        return self

    # @abstractmethod
    def __call__(self, context: Context, input: Input_contra) -> Output_co:
        raise NotImplementedError("Subclasses must implement this method")


class NodeRegistry:
    def __init__(self):
        self.types: dict[str, type["Node"]] = {}
        self.base_classes: list[type["Node"]] = []

    def register(self, type: str, cls: type["Node"]):
        if type in self.types and cls is not self.types[type]:
            raise ValueError(
                f'Node type "{type}" is already registered to a different class'
            )
        self.types[type] = cls
        print(f"Registering class {cls.__name__} as node type {type}")

    def get(self, type: str) -> type["Node"]:
        if type not in self.types:
            raise ValueError(f'Node type "{type}" is not registered')
        return self.types[type]

    def register_base(self, cls: type["Node"]):
        if cls not in self.base_classes:
            self.base_classes.append(cls)
            print(f"Registering class {cls.__name__} as base node type")

    def is_base_class(self, cls: type["Node"]) -> bool:
        return cls in self.base_classes


_registry = NodeRegistry()


__all__ = [
    "Node",
    "Params",
]
