import weakref
from abc import ABC

import typing_extensions as t

from flexplan.stations.base import Station


@t.final
class WorkerIntrospection:
    __slots__ = ("_station",)

    def __init__(
        self,
        *,
        station: Station,
    ):
        self._station = weakref.ref(station)

    @property
    def station(self) -> Station:
        return self._station()


class Worker(ABC):
    def __post_init__(self):
        ...

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

    def __finalize__(self):
        ...
