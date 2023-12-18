from ast import Call
from typing_extensions import (
    Callable,
    Generic,
    ParamSpec,
    Type,
    TypeVar,
    overload,
)

P = ParamSpec("P")
T = TypeVar("T")


class InstanceCreator(Generic[T]):
    __slots__ = ("type", "args", "kwargs")

    @overload
    def __init__(self, __anytype: Type[T], /, *args, **kwargs):
        ...

    @overload
    def __init__(self, __anytype: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs):
        ...

    def __init__(self, __anytype, /, *args, **kwargs):
        self.type = __anytype
        self.args = args
        self.kwargs = kwargs

    def create(self) -> T:
        return self.type(*self.args, **self.kwargs)
