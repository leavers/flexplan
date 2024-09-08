import atexit
from weakref import WeakSet

from typing_extensions import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from flexplan.datastructures.future import Future
    from flexplan.stations.base import Station


class Joinable(Protocol):
    def join(self) -> Any: ...


_futures: WeakSet = WeakSet()
_stations: WeakSet = WeakSet()
_joinable_items: WeakSet = WeakSet()


def stop_future_atexit(future: "Future") -> None:
    _futures.add(future)


def stop_joinable_atexit(future: "Joinable") -> None:
    _joinable_items.add(future)


def stop_station_atexit(station: "Station") -> None:
    _stations.add(station)


def _atexit_callback() -> None:
    for future in _futures:
        future.cancel()
    for station in _stations:
        station.stop()
    for joinable in _joinable_items:
        joinable.join()


atexit.register(_atexit_callback)
