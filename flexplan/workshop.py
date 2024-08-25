from typing_extensions import (
    Any,
    Callable,
    Concatenate,
    List,
    Optional,
    ParamSpec,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from flexplan.datastructures.deferredbox import DeferredBox
from flexplan.datastructures.future import Future
from flexplan.datastructures.instancecreator import Creator, InstanceCreator
from flexplan.messages.mail import Mail
from flexplan.messages.message import Message
from flexplan.stations.base import Station
from flexplan.stations.thread import ThreadStation
from flexplan.supervisor import Supervisor, SupervisorWorkbench
from flexplan.types import WorkerSpec
from flexplan.utils.identity import gen_worker_id
from flexplan.workbench.base import Workbench
from flexplan.workbench.loop import LoopWorkbench
from flexplan.workers.base import Worker

P = ParamSpec("P")
R = TypeVar("R")


class Workshop(ThreadStation):
    def __init__(self):
        super().__init__(
            workbench_creator=InstanceCreator(SupervisorWorkbench),
            worker_creator=InstanceCreator(Supervisor).bind(worker_specs=[]),
        )

    def register(
        self,
        worker: Union[Type[Worker], Creator[Worker]],
        name: Optional[str] = None,
        *,
        station: Optional[Union[Type[Station], Creator[Station], str]] = None,
        workbench: Optional[Union[Type[Workbench], Creator[Workbench]]] = None,
    ) -> str:
        if name is not None:
            if not isinstance(name, str):
                raise TypeError(f"Unexpected name type: {type(name)}")
            elif not name:
                raise ValueError("Name must be non-empty string")

        worker_creator: Creator[Worker]
        workbench_creator: Creator[Workbench]
        station_creator: Creator[Station]

        if isinstance(worker, InstanceCreator):
            worker_creator = worker
        elif issubclass(wk_t := cast(Type[Worker], worker), Worker):
            worker_creator = InstanceCreator(wk_t)
        else:
            raise TypeError(f"Unexpected worker type: {type(worker)}")

        if workbench is None:
            workbench_creator = InstanceCreator(LoopWorkbench)
        elif isinstance(workbench, InstanceCreator):
            workbench_creator = workbench
        elif issubclass(wb_t := cast(Type[Workbench], workbench), Workbench):
            workbench_creator = InstanceCreator(wb_t)
        else:
            raise TypeError(f"Unexpected workbench type: {type(workbench)}")

        # TODO: Move all station retrieval logic to a registry class
        if station is None:
            station_creator = InstanceCreator(ThreadStation).bind(
                workbench_creator=workbench_creator,
                worker_creator=worker_creator,
            )
        elif isinstance(station, str):
            if station == "thread":
                station_creator = InstanceCreator(ThreadStation)
            elif station == "process":
                from flexplan.stations.process import ProcessStation

                station_creator = InstanceCreator(ProcessStation)
            elif station == "fork":
                from flexplan.stations.process import ForkProcessStation

                station_creator = InstanceCreator(ForkProcessStation)
            elif station == "forkserver":
                from flexplan.stations.process import ForkServerProcessStation

                station_creator = InstanceCreator(ForkServerProcessStation)
            elif station == "spawn":
                from flexplan.stations.process import SpawnProcessStation

                station_creator = InstanceCreator(SpawnProcessStation)
            else:
                raise ValueError(f"Unexpected station name: {station}")
            station_creator = station_creator.bind(
                workbench_creator=workbench_creator,
                worker_creator=worker_creator,
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
        elif issubclass(st_t := cast(Type[Station], station), Station):
            station_creator = InstanceCreator(st_t).bind(
                workbench_creator=workbench_creator,
                worker_creator=worker_creator,
            )
        else:
            raise TypeError(f"Unexpected station type: {type(station)}")

        worker_id = gen_worker_id()
        worker_specs: List[WorkerSpec] = self._worker_creator.kwargs["worker_specs"]
        worker_specs.append((worker_id, name, station_creator))
        return worker_id

    @overload
    def submit(
        self,
        fn: Callable[Concatenate[Any, P], R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]: ...

    @overload
    def submit(
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[R]: ...

    @overload
    def submit(self, fn: "Message", /) -> Future: ...

    def submit(
        self,
        fn,
        /,
        *args,
        **kwargs,
    ) -> Future:
        if isinstance(fn, Message):
            message = fn
        else:
            if args or kwargs:
                raise ValueError(
                    "No args or kwargs should be specified if a Message is submitted"
                )
            message = Message(fn).params(*args, **kwargs)

        box: DeferredBox[Future] = DeferredBox()
        self.send(Mail.new(message=message, future=box))
        future = box.get()
        return future
