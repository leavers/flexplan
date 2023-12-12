from abc import ABC
from concurrent.futures import Future

import typing_extensions as t


class Message(ABC):
    def __init__(self, invocation: t.Callable):
        self._invocation = invocation

    def to(self, tags: t.Iterable[str]) -> t.Self:
        return self

    def params(self, *args, **kwargs) -> t.Self:
        return self

    def submit(self) -> Future:
        ...

    def dispatch(self) -> None:
        ...


class Packet(ABC):
    def __init__(
        self,
    ):
        ...
