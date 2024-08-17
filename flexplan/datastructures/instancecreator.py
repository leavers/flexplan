from abc import abstractmethod
from inspect import isfunction

from typing_extensions import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

T = TypeVar("T")
T_cov = TypeVar("T_cov", covariant=True)


class Creator(Generic[T_cov]):
    @property
    @abstractmethod
    def type(self) -> Type[T_cov]: ...

    @property
    @abstractmethod
    def args(self) -> Tuple[Any, ...]: ...

    @property
    @abstractmethod
    def kwargs(self) -> Dict[str, Any]: ...

    @abstractmethod
    def create(self) -> T_cov: ...


class InstanceCreator(Creator[T]):
    __slots__ = ("_creator", "_type", "_args", "_kwargs")

    @overload
    def __init__(self, creator: Type[T]): ...

    @overload
    def __init__(self, creator: Callable[..., T], type_: Type[T]): ...

    def __init__(
        self,
        creator: Union[Callable[..., T], Type[T]],
        type_: Optional[Type[T]] = None,
    ):
        self._type: Type[T]
        self._creator = creator
        if isfunction(creator):
            if type_ is None:
                raise ValueError(
                    "Param `type` must not be None when creator is a function"
                )
            self._type = type_
        else:
            if type_ is not None:
                raise ValueError(
                    "Param `type` must be None when creator is not a function"
                )
            self._type = cast(Type[T], creator)
        self._args: Tuple[Any, ...] = ()
        self._kwargs: Dict[str, Any] = {}

    def bind(self, *args, **kwargs) -> "InstanceCreator[T]":
        self._args = args
        self._kwargs = kwargs
        return self

    @property
    def type(self) -> Type[T]:
        return self._type

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def create(self) -> T:
        print(f"Create {self._creator}, {self.args}, {self.kwargs}")
        instance = self._creator(*self.args, **self.kwargs)  # type: ignore
        print(f"Created {instance}")
        return instance
