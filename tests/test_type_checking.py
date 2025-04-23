from enum import Enum
from typing import Any, Optional, TypeVar, Union, Literal, Generic
from pydantic import BaseModel
import pytest

from workflow_engine.utils.assign import is_assignable, expand_type

# Test fixtures
class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"

class Animal:
    pass

class Dog(Animal):
    pass

class Cat(Animal):
    pass

class SimpleModel(BaseModel):
    name: str
    age: int

class NestedModel(BaseModel):
    simple: SimpleModel
    color: Color

class GenericModel(BaseModel):
    value: Any

T = TypeVar('T')

class GenericClass(Generic[T]):
    pass

@pytest.mark.unit
def test_basic_types():
    assert is_assignable(int, int)
    assert is_assignable(str, str)
    assert is_assignable(bool, bool)
    assert not is_assignable(int, str)
    assert not is_assignable(str, int)
    assert is_assignable(Dog, Animal)
    assert not is_assignable(Animal, Dog)

@pytest.mark.unit
def test_none_and_any():
    assert is_assignable(None, None)
    assert is_assignable(Any, Any)
    assert is_assignable(None, Any)
    assert not is_assignable(Any, None)
    assert is_assignable(int, Any)
    assert is_assignable(str, Any)
    assert is_assignable(list[int], Any)

@pytest.mark.unit
def test_literals():
    assert is_assignable(Literal[1], int)
    assert is_assignable(Literal["hello"], str)
    assert is_assignable(Literal[True], bool)
    assert not is_assignable(Literal[1], str)
    assert not is_assignable(Literal["hello"], int)

@pytest.mark.unit
def test_enums():
    assert is_assignable(Color, Enum)
    assert is_assignable(Literal[Color.RED], Color)
    assert not is_assignable(Color, int)
    assert not is_assignable(int, Color)

@pytest.mark.unit
def test_optional():
    assert is_assignable(Optional[int], Union[int, None])
    assert is_assignable(int, Optional[int])
    assert is_assignable(None, Optional[int])
    assert not is_assignable(Optional[int], int)

@pytest.mark.unit
def test_unions():
    assert is_assignable(int, Union[int, str])
    assert is_assignable(int, int | str)
    assert is_assignable(str, int | str)
    assert is_assignable(int | str, int | str)
    assert is_assignable(bool | str, bool | int | str)

    assert not is_assignable(int | str, int)
    assert not is_assignable(int | str, str)
    assert not is_assignable(float, int | str)
    assert not is_assignable(bool | str, bool | int)
    assert is_assignable(list[int], list[int] | list[str])
    assert not is_assignable(list[float], list[int] | list[str])

@pytest.mark.unit
def test_pydantic_models():
    assert is_assignable(SimpleModel, BaseModel)
    assert is_assignable(NestedModel, BaseModel)
    assert not is_assignable(SimpleModel, NestedModel)
    assert not is_assignable(NestedModel, SimpleModel)

@pytest.mark.unit
def test_generic_types():
    # Basic generic types
    assert is_assignable(list[int], list[int])
    assert is_assignable(list[int], list[Any], covariant=True)
    assert not is_assignable(list[str], list[int])
    assert not is_assignable(list[int], list[str])

    # Generic types with inheritance
    assert not is_assignable(list[Dog], list[Animal])
    assert is_assignable(list[Dog], list[Animal], covariant=True)

    # Different generic types
    assert not is_assignable(list[int], dict[str, int])

    # Generic classes
    assert is_assignable(GenericClass[int], GenericClass[int])
    assert not is_assignable(GenericClass[int], GenericClass[str])

@pytest.mark.unit
def test_nested_generics():
    assert is_assignable(list[list[int]], list[list[int]])
    assert is_assignable(dict[str, list[int]], dict[str, list[int]])
    assert not is_assignable(list[list[int]], list[list[str]])
    assert not is_assignable(dict[str, list[int]], dict[str, list[str]])

@pytest.mark.unit
def test_complex_nested_types():
    complex_type = dict[str, list[Optional[Union[int, str]]]]
    assert is_assignable(complex_type, complex_type)
    assert is_assignable(dict[str, list[Optional[Union[int, str]]]], dict[str, list[Any]], covariant=True)
    assert not is_assignable(complex_type, dict[str, list[int]])

@pytest.mark.unit
def test_pydantic_with_generics():
    assert is_assignable(GenericModel, BaseModel)

@pytest.mark.unit
def test_expand_type():
    # Test basic type expansion
    expanded = expand_type(int)
    assert len(expanded) == 1
    assert expanded[0] == (False, int)

    # Test Optional expansion
    expanded = expand_type(Optional[int])
    assert len(expanded) == 2
    assert (True, None) in expanded
    assert (False, int) in expanded

    # Test Union expansion
    expanded = expand_type(Union[int, str])
    assert len(expanded) == 2
    assert (False, int) in expanded
    assert (False, str) in expanded

    # Test Literal expansion
    expanded = expand_type(Literal[1, 2, 3])
    assert len(expanded) == 3
    assert (True, 1) in expanded
    assert (True, 2) in expanded
    assert (True, 3) in expanded

@pytest.mark.unit
def test_edge_cases():
    # Test with type objects
    assert is_assignable(type, type)
    assert is_assignable(object, object)

    # Test with built-in types
    assert is_assignable(list, list)
    assert is_assignable(dict, dict)

    # Test with empty types
    assert is_assignable(list[Any], list[Any])
    assert is_assignable(dict[Any, Any], dict[Any, Any])

@pytest.mark.unit
def test_covariant_behavior():
    # list is covariant in its type parameter
    assert is_assignable(list[int], list[Union[int, float]], covariant=True)
    assert not is_assignable(list[int], list[Union[int, float]], covariant=False)

    # dict is covariant in both type parameters
    # NOTE: this is not classically true, but in the read-only context of
    # workflow_engine, this assumption is actually very reasonable.
    assert is_assignable(dict[str, int], dict[str, Union[int, float]], covariant=True)
    assert is_assignable(dict[str, int], dict[Union[str, bytes], int], covariant=True)
    assert is_assignable(dict[str, int], dict[Union[str, bytes], Union[int, float]], covariant=True)
