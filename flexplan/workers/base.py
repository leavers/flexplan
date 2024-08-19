import weakref
from abc import ABC
from types import TracebackType

import typing_extensions as t

from flexplan.stations.base import Station

if t.TYPE_CHECKING:
    from flexplan.messages.message import Message


@t.final
class WorkerIntrospection:
    __slots__ = ("_station",)

    def __init__(
        self,
        *,
        station: Station,
    ):
        self._station = weakref.proxy(station)

    @property
    def station(self) -> Station:
        assert self._station is not None
        return self._station


class Worker(ABC):
    def __post_init__(self): ...

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None: ...

    def on(self, message: "Message"): ...

    @property
    def introspection(self) -> WorkerIntrospection:
        if not hasattr(self, "_intro7n"):
            raise AttributeError("introspection not set")
        return getattr(self, "_intro7n")

    @introspection.setter
    def introspection(self, value: WorkerIntrospection):
        if hasattr(self, "_intro7n"):
            raise AttributeError("introspection already set")
        setattr(self, "_intro7n", value)

    def __finalize__(self): ...
