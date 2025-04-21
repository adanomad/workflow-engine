# workflow_engine/core/node.py
from typing import (
    Any,
    Generic,
    _LiteralGenericAlias, # type: ignore
    Mapping,
    Type,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict, model_validator

from .context import Context
from .data import Data, Empty, Input_contra, Output_co


Params_co = TypeVar("Params_co", bound=Data)
T = TypeVar("T")

class Node(BaseModel, Generic[Input_contra, Output_co, Params_co]):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    type: str # should be a literal string for concrete subclasses
    id: str
    # contains any extra fields for configuring the node
    params: Params_co = Empty() # type: ignore

    @property
    # @abstractmethod
    def input_type(self) -> Type[Input_contra]: # type: ignore (contravariant return type)
        # return Data to spare users from having to specify the input type on
        # nodes that don't have any input fields
        return Empty # type: ignore

    @property
    # @abstractmethod
    def output_type(self) -> Type[Output_co]:
        # return Data to spare users from having to specify the output type on
        # nodes that don't have any output fields
        return Empty # type: ignore

    @property
    def input_fields(self) -> Mapping[str, Type[Any]]:
        return self.input_type.__annotations__

    @property
    def output_fields(self) -> Mapping[str, Type[Any]]:
        return self.output_type.__annotations__

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Get type annotation of 'type' field from class annotations
        type_annotation = cls.__annotations__.get("type", None)
        if type_annotation is not None:
            name = cls.__name__
            assert name.endswith("Node"), name

            assert isinstance(type_annotation, _LiteralGenericAlias)
            type_name, = type_annotation.__args__
            assert isinstance(type_name, str), type_name
            assert type_name == name.removesuffix("Node")
            _registry.register(type_name, cls)

    @model_validator(mode="after") # type: ignore
    def _to_subclass(self):
        # NOTE: This trick only works if Node itself can be instantiated, so we
        # cannot make it an ABC even though it has many unimplemented methods.
        if self.__class__ is Node:
            cls = _registry.get(self.type)
            if cls is None:
                raise ValueError(f'Node type "{self.type}" is not registered')
            print(self, self.model_dump())
            return cls.model_validate(self.model_dump())
        return self

    # @abstractmethod
    def __call__(self, context: Context, input: Input_contra) -> Output_co:
        raise NotImplementedError("Subclasses must implement this method")


class NodeRegistry:
    def __init__(self):
        self.types: dict[str, type["Node"]] = {}

    def register(self, type: str, cls: type["Node"]):
        if type in self.types and cls is not self.types[type]:
            raise ValueError(f'Node type "{type}" is already registered to a different class')
        self.types[type] = cls
        print(f"Registering class {cls.__name__} as node type {type}")

    def get(self, type: str) -> type["Node"]:
        if type not in self.types:
            raise ValueError(f'Node type "{type}" is not registered')
        return self.types[type]

_registry = NodeRegistry()


__all__ = [
    "Node",
]
