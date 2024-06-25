from typing_extensions import (
    TYPE_CHECKING,
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

from flexplan.datastructures.future import Future
from flexplan.datastructures.instancecreator import Creator, InstanceCreator
from flexplan.stations.base import Station
from flexplan.stations.thread import ThreadStation
from flexplan.supervisor import Supervisor, SupervisorWorkbench
from flexplan.utils.identity import gen_worker_id
from flexplan.workbench.base import Workbench
from flexplan.workbench.loop import LoopWorkbench
from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from flexplan.messages.message import Message
    from flexplan.types import WorkerSpec


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
        workbench: Optional[Union[Type[Workbench], Creator[Workbench]]] = None,
        station: Optional[Union[Type[Station], Creator[Station]]] = None,
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

        if station is None:
            station_creator = InstanceCreator(ThreadStation).bind(
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
