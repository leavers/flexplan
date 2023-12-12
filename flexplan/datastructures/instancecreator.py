import typing_extensions as t

T = t.TypeVar("T")


class InstanceCreator(t.Generic[T]):
    __slots__ = ("type", "args", "kwargs")

    def __init__(
        self,
        __anytype: t.Union[t.Type[T], t.Callable[..., T]],
        /,
        *args,
        **kwargs,
    ):
        self.type = __anytype
        self.args = args
        self.kwargs = kwargs

    def create(self) -> T:
        return self.type(*self.args, **self.kwargs)
