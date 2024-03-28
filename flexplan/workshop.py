from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    List,
    Optional,
    ParamSpec,
    Self,
    Type,
    TypeVar,
    Union,
    overload,
)

from flexplan.datastructures.future import Future
from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.stations.base import Station
from flexplan.stations.thread import ThreadStation
from flexplan.supervisor import Supervisor, SupervisorWorkbench
from flexplan.workbench.base import Workbench
from flexplan.workbench.loop import LoopWorkbench
from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from flexplan.messages.message import Message
    from flexplan.types import StationSpec


P = ParamSpec("P")
R = TypeVar("R")


class Workshop(ThreadStation):
    def __init__(self):
        super().__init__(
            workbench_creator=InstanceCreator(SupervisorWorkbench),
            worker_creator=InstanceCreator(Supervisor, station_specs=[]),
        )

    def register(
        self,
        worker: Union[Type[Worker], InstanceCreator[Worker]],
        name: Optional[str] = None,
        *,
        workbench: Optional[Union[Type[Workbench], InstanceCreator[Workbench]]] = None,
        station: Optional[Union[Type[Station], InstanceCreator[Station]]] = None,
    ) -> Self:
        if name is not None:
            if not isinstance(name, str):
                raise TypeError(f"Unexpected name type: {type(name)}")
            elif not name:
                raise ValueError("Name must be non-empty string")

        worker_creator: InstanceCreator[Worker]
        workbench_creator: InstanceCreator[Workbench]
        station_creator: InstanceCreator[Station]

        if isinstance(worker, InstanceCreator):
            worker_creator = worker
        elif issubclass(worker, Worker):
            worker_creator = InstanceCreator(worker)
        else:
            raise TypeError(f"Unexpected worker type: {type(worker)}")

        if workbench is None:
            workbench_creator = InstanceCreator(LoopWorkbench)
        elif isinstance(workbench, InstanceCreator):
            workbench_creator = workbench
        elif issubclass(workbench, Workbench):
            workbench_creator = InstanceCreator(workbench)  # type: ignore[assignment]
        else:
            raise TypeError(f"Unexpected workbench type: {type(workbench)}")

        if station is None:
            station_creator = InstanceCreator(
                ThreadStation,
                worker_creator=worker_creator,
                workbench_creator=workbench_creator,
            )
        elif isinstance(station, InstanceCreator):
            kwargs = station.kwargs
            if "worker_creator" in kwargs:
                raise ValueError("Cannot specify worker_creator in station_creator")
            if "workbench_creator" in kwargs:
                raise ValueError("Cannot specify workbench_creator in station_creator")
            kwargs["worker_creator"] = worker_creator
            kwargs["workbench_creator"] = workbench_creator
            station_creator = station
        elif issubclass(station, Station):
            station_creator = InstanceCreator(
                station,
                worker_creator=worker_creator,
                workbench_creator=workbench_creator,
            )
        else:
            raise TypeError(f"Unexpected station type: {type(station)}")

        station_specs: List[StationSpec] = self._worker_creator.kwargs["station_specs"]
        station_specs.append((name, station_creator))
        return self

    @overload
    def submit(
        self,
        fn: Callable[Concatenate[Any, P], R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        ...

    @overload
    def submit(
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]:
        ...

    @overload
    def submit(self, fn: "Message", /) -> Future:
        ...

    def submit(
        self,
        fn,
        /,
        *args,
        **kwargs,
    ) -> Future:
        from flexplan.messages.mail import Mail
        from flexplan.messages.message import Message

        if isinstance(fn, Message):
            message = fn
        else:
            if args or kwargs:
                raise ValueError(
                    "No args or kwargs should be specified if a Message is submitted"
                )
            message = Message(fn).params(*args, **kwargs)

        future: Future = Future()
        self.send(Mail.new(message=message, future=future))
        return future
