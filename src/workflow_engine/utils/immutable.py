from contextlib import contextmanager
from typing import Any, Generic, Self, TypeVar
from pydantic import BaseModel, ConfigDict, RootModel


_immutable_model_config = ConfigDict(
    frozen=True,
    revalidate_instances="always",
    validate_assignment=True,
)


class ImmutableMixin:
    """
    A base model that is immutable.
    """

    def __init_subclass__(cls, **kwargs):
        """
        Make a deep copy of the model config so that each subclass can be
        unfrozen independently.
        """
        cls.model_config = cls.model_config | _immutable_model_config
        super().__init_subclass__(**kwargs)

    @contextmanager
    def unfreeze(self):
        """
        Unfreeze the model for the duration of the context manager.
        This affects all instances of the same subclass.
        """
        try:
            self.model_config["frozen"] = False
            yield self
        finally:
            self.model_config["frozen"] = True

    def model_update(self, **kwargs: Any) -> Self:
        return self.model_copy(update=kwargs)  # type: ignore


class ImmutableBaseModel(BaseModel, ImmutableMixin):
    pass


T = TypeVar("T")


class ImmutableRootModel(RootModel[T], ImmutableMixin, Generic[T]):
    pass


__all__ = [
    "ImmutableBaseModel",
]
