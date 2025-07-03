from collections.abc import Mapping, Sequence
from logging import getLogger
from typing import (
    Callable,
    ClassVar,
    Generic,
    TYPE_CHECKING,
    Self,
    Type,
    TypeVar,
    TypeAliasType,
    cast,
    get_args,
    get_origin,
)

from pydantic import ConfigDict, PrivateAttr, RootModel

if TYPE_CHECKING:
    from .context import Context


logger = getLogger(__name__)


T = TypeVar("T")
V = TypeVar("V", bound="Value")


OriginAndArgs = TypeAliasType("OriginAndArgs", tuple[str, tuple[Type["Value"], ...]])
OriginAndArgsRecursive = TypeAliasType(
    "OriginAndArgsRecursive", tuple[str, tuple["OriginAndArgsRecursive", ...]]
)
Caster = TypeAliasType("Caster", Callable[["Context", "Value"], "Value"])
GenericCaster = TypeAliasType(
    "GenericCaster",
    Callable[[Sequence[Type["Value"]], Sequence[Type["Value"]]], Caster],
)


def get_origin_and_args(t: Type) -> tuple[str, tuple[Type, ...]]:
    # Pydantic RootModels don't play nice with get_origin and get_args, so we
    # get the root type directly from the model fields.
    if issubclass(t, RootModel):
        info = t.__pydantic_generic_metadata__
        origin = info["origin"]
        args = info["args"]
    else:
        origin = get_origin(t)
        args = get_args(t)

    if origin is None:
        assert len(args) == 0
        return t.__name__, ()
    else:
        assert len(args) > 0
        return origin.__name__, args


def get_origin_and_args_recursive(t: Type["Value"]) -> OriginAndArgsRecursive:
    origin, args = get_origin_and_args(t)
    return origin, tuple(get_origin_and_args_recursive(arg) for arg in args)


class Value(RootModel[T], Generic[T]):
    """
    Wraps an arbitrary read-only value which can be passed as input to a node.
    Allows users to register arbitrary functions to cast values to other types.
    """

    model_config = ConfigDict(frozen=True)

    # these properties force us to implement __eq__ and __hash__ to ignore them
    _casters: ClassVar[dict[str, GenericCaster]] = {}
    _resolved_casters: ClassVar[dict[str, GenericCaster] | None] = None
    _cast_cache: dict[OriginAndArgsRecursive, "Value"] = PrivateAttr(
        default_factory=dict,
    )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # reinitialize for each subclass so it doesn't just reference the parent
        cls._casters = {}
        cls._resolved_casters = None

    @classmethod
    def _get_casters(cls) -> dict[str, GenericCaster]:
        """
        Get all converters for this class, including those inherited from parent classes.
        This method dynamically builds the converter dictionary by merging converters
        from the current class and all its parent classes.
        """
        if cls._resolved_casters is not None:
            return cls._resolved_casters

        resolved_casters: dict[str, GenericCaster] = cls._casters.copy()

        # Add converters from all classes in MRO order
        # (starting from this class, then parents, then parents of parents, ...)
        for parent in cls.__bases__:
            if issubclass(parent, Value):
                parent_casters = parent._get_casters()
            else:
                continue

            for origin, caster in parent_casters.items():
                # converters in the child class override those in the parent
                # class
                if origin not in resolved_casters:
                    resolved_casters[origin] = caster

        cls._resolved_casters = resolved_casters
        return resolved_casters

    @classmethod
    def add_cast(cls, t: Type[V], caster: GenericCaster):
        """
        Register a possible type cast from this class to the class T.
        """
        if cls._resolved_casters is not None:
            raise RuntimeError(
                f"Cannot add casters for {cls.__name__} after it has been used to cast values"
            )

        origin, _ = get_origin_and_args(t)

        assert origin not in cls._casters, (
            f"Type caster from {cls.__name__} to {origin} already registered"
        )
        cls._casters[origin] = caster

    @classmethod
    def get_caster(cls, t: Type[V]) -> Caster | None:
        _, from_args = get_origin_and_args(cls)
        to_name, to_args = get_origin_and_args(t)

        converters = cls._get_casters()
        if to_name in converters:
            cast_fn = converters[to_name]
            # Check if this is a generic cast function (takes 2 arguments) or simple cast function
            if hasattr(cast_fn, "__code__") and cast_fn.__code__.co_argcount == 2:
                # Generic cast function
                cast_fn = cast(GenericCaster, cast_fn)
                try:
                    return cast_fn(from_args, to_args)
                except Exception:
                    logger.exception(f"Cannot instantiate cast function for type {t}")
                    return None
            else:
                # Simple cast function
                return cast(Caster, cast_fn)

        if issubclass(cls, t):
            return lambda ctx, v: v

        return None

    @classmethod
    def can_cast_to(cls, t: Type[V]) -> bool:
        return cls.get_caster(t) is not None

    def __eq__(self, other):
        if not isinstance(other, Value):
            return False
        return self.root == other.root

    def __hash__(self):
        return hash(self.root)

    def cast_to(self, t: Type[V], *, context: "Context") -> V:
        key = get_origin_and_args_recursive(t)
        if key in self._cast_cache:
            casted = self._cast_cache[key]
            assert isinstance(casted, t)
            return casted

        cast_fn = self.get_caster(t)
        if cast_fn is not None:
            casted = cast_fn(context, self)
            assert isinstance(casted, t)
            self._cast_cache[key] = casted
            return casted

        raise ValueError(f"Cannot convert {self} to {t}")

    @classmethod
    def cast_from(cls, v: "Value", *, context: "Context") -> Self:
        return v.cast_to(cls, context=context)


class StringValue(Value[str]):
    pass


class IntegerValue(Value[int]):
    pass


class FloatValue(Value[float]):
    pass


V = TypeVar("V", bound=Value)


class SequenceValue(Value[Sequence[V]], Generic[V]):
    root: Sequence[V]


class StringMapValue(Value[Mapping[str, V]], Generic[V]):
    root: Mapping[str, V]


Value.add_cast(StringValue, lambda S, T: lambda ctx, v: StringValue(root=str(v.root)))
IntegerValue.add_cast(
    FloatValue, lambda S, T: lambda ctx, v: FloatValue(root=float(v.root))
)
StringValue.add_cast(
    IntegerValue, lambda S, T: lambda ctx, v: IntegerValue.model_validate_json(v.root)
)
StringValue.add_cast(
    FloatValue, lambda S, T: lambda ctx, v: FloatValue.model_validate_json(v.root)
)


def cast_sequence(Ss: Sequence[Type[Value]], Ts: Sequence[Type[V]]) -> Caster:
    (S,) = Ss
    (T,) = Ts
    assert S.can_cast_to(T), f"Cannot cast item type {S} to {T}"
    return lambda ctx, value: SequenceValue[T](
        root=[x.cast_to(T, context=ctx) for x in value.root]
    )


def cast_map(Ss: Sequence[Type[Value]], Ts: Sequence[Type[V]]) -> Caster:
    (S,) = Ss
    (T,) = Ts
    assert S.can_cast_to(T), f"Cannot cast item type {S} to {T}"
    return lambda ctx, value: StringMapValue[T](
        root={k: v.cast_to(T, context=ctx) for k, v in value.root.items()}  # type: ignore
    )


SequenceValue.add_cast(SequenceValue, cast_sequence)
StringMapValue.add_cast(StringMapValue, cast_map)
