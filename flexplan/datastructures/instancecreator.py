from typing_extensions import (
    Any,
    Callable,
    Dict,
    Generic,
    ParamSpec,
    Protocol,
    Tuple,
    TypeVar,
)

P = ParamSpec("P")
T = TypeVar("T")
T_cov = TypeVar("T_cov", covariant=True)


class Creator(Protocol[T_cov]):
    @property
    def type(self) -> Callable[..., T_cov]:
        ...

    @property
    def args(self) -> Tuple[Any, ...]:
        ...

    @property
    def kwargs(self) -> Dict[str, Any]:
        ...

    def create(self) -> T_cov:
        ...


class InstanceCreator(Generic[P, T]):
    __slots__ = ("type", "args", "kwargs")

    def __init__(self, __type: Callable[P, T]):
        self.type = __type
        self.args: Tuple[Any, ...] = ()
        self.kwargs: Dict[str, Any] = {}

    def bind(self, *args: P.args, **kwargs: P.kwargs) -> "Creator[T]":
        self.args = args
        self.kwargs = kwargs
        return self

    def create(self) -> T:
        return self.type(*self.args, **self.kwargs)  # type: ignore
