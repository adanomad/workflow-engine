# my_workflow_engine/registry.py
from typing import Dict, Callable, Any, TypeVar, Optional, Union
from dataclasses import dataclass, field
import inspect
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Callable)


@dataclass
class ParameterMetadata:
    name: str
    annotation: Any
    default: Any
    kind: inspect._ParameterKind
    annotation_str: str = field(init=False)

    def __post_init__(self):
        self.annotation_str = str(self.annotation)


@dataclass
class FunctionMetadata:
    name: str
    description: str
    parameters: Dict[str, ParameterMetadata] = field(default_factory=dict)
    return_annotation: Any = None
    is_async: bool = False
    return_annotation_str: str = field(init=False)

    def __post_init__(self):
        self.return_annotation_str = str(self.return_annotation)


class FunctionRegistry:
    def __init__(self):
        self._functions: Dict[str, Callable] = {}
        self._metadata: Dict[str, FunctionMetadata] = {}

    def register(
        self, name: Optional[str] = None, description: str = ""
    ) -> Callable[[T], T]:
        """Decorator to register functions and store their metadata."""

        def decorator(func: T) -> T:
            func_name = name or func.__name__
            if func_name in self._functions:
                # Use warning level for redefinition
                logger.warning(
                    f"Function '{func_name}' is being redefined in the registry."
                )

            is_async = inspect.iscoroutinefunction(func)
            self._functions[func_name] = func  # Store the original function

            sig = inspect.signature(func)
            params_meta = {}
            for param_name, param in sig.parameters.items():
                params_meta[param_name] = ParameterMetadata(
                    name=param_name,
                    annotation=(
                        param.annotation
                        if param.annotation != inspect.Parameter.empty
                        else Any
                    ),
                    default=(
                        param.default
                        if param.default != inspect.Parameter.empty
                        else inspect.Parameter.empty
                    ),
                    kind=param.kind,
                )

            func_description = description or inspect.getdoc(func) or ""
            return_annotation = (
                sig.return_annotation
                if sig.return_annotation != inspect.Signature.empty
                else Any
            )

            self._metadata[func_name] = FunctionMetadata(
                name=func_name,
                description=func_description,
                parameters=params_meta,
                return_annotation=return_annotation,
                is_async=is_async,
            )

            logger.debug(f"Registered function '{func_name}' (async: {is_async})")
            return func

        return decorator

    def get_function(self, name: str) -> Optional[Callable]:
        """Get a registered function by name"""
        return self._functions.get(name)

    def get_metadata(self, name: str) -> Optional[FunctionMetadata]:
        """Get metadata for a registered function"""
        return self._metadata.get(name)

    def list_functions(self) -> Dict[str, FunctionMetadata]:
        """List all registered functions and their metadata"""
        return self._metadata.copy()


registry = FunctionRegistry()
