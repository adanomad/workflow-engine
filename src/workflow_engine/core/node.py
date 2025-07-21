# workflow_engine/core/node.py
import asyncio
import logging
from collections.abc import Mapping
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Generic,
    Self,
    Type,
    TypeVar,
    Unpack,
    _LiteralGenericAlias,  # type: ignore
)

from overrides import final
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from ..utils.generic import get_base
from .data import (
    Data,
    DataMapping,
    Input_contra,
    Output_co,
    get_data_fields,
)
from .error import NodeException, UserException
from .schema import ObjectJSONSchema
from .value import Value, ValueType

if TYPE_CHECKING:
    from .context import Context
    from .workflow import Workflow

logger = logging.getLogger(__name__)


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
    def input_fields(self) -> Mapping[str, tuple[ValueType, bool]]:
        return get_data_fields(self.input_type)

    @property
    def output_fields(self) -> Mapping[str, tuple[ValueType, bool]]:
        return get_data_fields(self.output_type)

    @cached_property
    def input_schema(self) -> ObjectJSONSchema:
        return self.input_type.to_schema()

    @cached_property
    def output_schema(self) -> ObjectJSONSchema:
        return self.output_type.to_schema()

    def __init_subclass__(cls, **kwargs: Unpack[ConfigDict]):
        super().__init_subclass__(**kwargs)  # type: ignore

        cls = get_base(cls)
        name = cls.__name__
        assert name.endswith("Node"), name
        type_annotation = cls.__annotations__.get("type", None)
        if type_annotation is None or not isinstance(
            type_annotation, _LiteralGenericAlias
        ):
            _node_registry.register_base(cls)
        else:
            (type_name,) = type_annotation.__args__
            assert isinstance(type_name, str), type_name
            assert type_name == name.removesuffix("Node")
            _node_registry.register(type_name, cls)

    @model_validator(mode="after")  # type: ignore
    def _to_subclass(self):
        """
        Replaces the Node object with an instance of the registered subclass.
        """
        # HACK: This trick only works if the base class can be instantiated, so
        # we cannot make it an ABC even if it has unimplemented methods.
        if _node_registry.is_base_class(self.__class__):
            cls = _node_registry.get(self.type)
            if cls is None:
                raise ValueError(f'Node type "{self.type}" is not registered')
            return cls.model_validate(self.model_dump())
        return self

    async def _cast_input(
        self,
        input: DataMapping,
        context: "Context",
    ) -> Input_contra:  # type: ignore (contravariant return type)
        allow_extra_input = (
            self.input_type.model_config.get("extra", "forbid") == "allow"
        )

        # Validate all inputs first
        for key, value in input.items():
            if key not in self.input_fields and allow_extra_input:
                continue
            input_type, _ = self.input_fields[key]
            if not value.can_cast_to(input_type):
                raise UserException(
                    f"Input {value} for node {self.id} is invalid: {value} is not assignable to {input_type}"
                )

        # Cast all inputs in parallel
        cast_tasks: list[Awaitable[Value]] = []
        keys: list[str] = []
        for key, value in input.items():
            if key not in self.input_fields and allow_extra_input:
                continue
            input_type, _ = self.input_fields[key]  # type: ignore
            cast_tasks.append(value.cast_to(input_type, context=context))
            keys.append(key)

        casted_values = await asyncio.gather(*cast_tasks)

        # Build the result dictionary
        casted_input: dict[str, Value] = {}
        for key, casted_value in zip(keys, casted_values):
            casted_input[key] = casted_value

        try:
            return self.input_type.model_validate(casted_input)
        except ValidationError as e:
            raise UserException(
                f"Input {casted_input} for node {self.id} is invalid: {e}"
            )

    @final
    async def __call__(
        self,
        context: "Context",
        input: DataMapping,
    ) -> "DataMapping | Workflow":
        try:
            logger.info("Starting node %s", self.id)
            output = await context.on_node_start(node=self, input=input)
            if output is not None:
                return output
            try:
                input_obj = await self._cast_input(input, context)
            except ValidationError as e:
                raise UserException(f"Input {input} for node {self.id} is invalid: {e}")
            output_obj = await self.run(context, input_obj)

            from .workflow import Workflow  # lazy to avoid circular import

            if isinstance(output_obj, Workflow):
                output = output_obj
                # TODO: once that workflow eventually finishes running, its
                # output should be the output of this node, and we should call
                # context.on_node_finish.
            else:
                output = await context.on_node_finish(
                    node=self,
                    input=input,
                    output=output_obj.to_dict(),
                )
            logger.info("Finished node %s", self.id)
            return output
        except Exception as e:
            # In subclasses, you don't have to worry about logging the error,
            # since it'll be logged here.
            logger.exception("Error in node %s", self.id)
            e = await context.on_node_error(node=self, input=input, exception=e)
            if isinstance(e, Mapping):
                logger.exception(
                    "Error absorbed by context and replaced with output %s", e
                )
                return e
            else:
                assert isinstance(e, Exception)
                raise NodeException(self.id) from e

    # HACK: we can't actaully make this method abstract because we need to
    # instantiate Nodes for deserialization
    # @abstractmethod
    async def run(
        self,
        context: "Context",
        input: Input_contra,
    ) -> "Output_co | Workflow":
        raise NotImplementedError("Subclasses must implement this method")

    def with_namespace(self, namespace: str) -> Self:
        """
        Create a copy of this node with a namespaced ID.

        Args:
            namespace: The namespace to prefix the node ID with

        Returns:
            A new Node with ID '{namespace}/{self.id}'
        """
        return self.model_copy(update={"id": f"{namespace}/{self.id}"})


class NodeRegistry:
    def __init__(self):
        self.types: dict[str, Type["Node"]] = {}
        self.base_classes: list[Type["Node"]] = []

    def register(self, type: str, cls: Type["Node"]):
        if type in self.types and cls is not self.types[type]:
            raise ValueError(
                f'Node type "{type}" is already registered to a different class'
            )
        self.types[type] = cls
        logger.info("Registering class %s as node type %s", cls.__name__, type)

    def get(self, type: str) -> Type["Node"]:
        if type not in self.types:
            raise ValueError(f'Node type "{type}" is not registered')
        return self.types[type]

    def register_base(self, cls: Type["Node"]):
        if cls not in self.base_classes:
            self.base_classes.append(cls)
            logger.info("Registering class %s as base node type", cls.__name__)

    def is_base_class(self, cls: Type["Node"]) -> bool:
        return cls in self.base_classes


_node_registry = NodeRegistry()


__all__ = [
    "Node",
    "Params",
]
