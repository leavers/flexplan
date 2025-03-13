from typing_extensions import (
    Any,
    Callable,
    Concatenate,
    Dict,
    List,
    Literal,
    Optional,
    ParamSpec,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
    override,
)

from flexplan.datastructures.deferredbox import DeferredBox
from flexplan.datastructures.future import Future
from flexplan.datastructures.instancecreator import Creator, InstanceCreator
from flexplan.messages.mail import Mail
from flexplan.messages.message import Message
from flexplan.stations.base import Station
from flexplan.stations.process import (
    ForkProcessStation,
    ForkServerProcessStation,
    ProcessStation,
    SpawnProcessStation,
)
from flexplan.stations.thread import ThreadStation
from flexplan.supervisor import Supervisor, SupervisorWorkbench
from flexplan.types import WorkerSpec
from flexplan.utils.identity import gen_worker_id
from flexplan.workbench.base import Workbench
from flexplan.workbench.loop import LoopWorkbench

__all__ = (
    "Workshop",
    "workshop_registry",
)


P = ParamSpec("P")
R = TypeVar("R")


class WorkshopRegistry:
    _station_specs: Dict[str, Type[Station]] = {
        "thread": ThreadStation,
        "process": ProcessStation,
        "fork": ForkProcessStation,
        "forkserver": ForkServerProcessStation,
        "spawn": SpawnProcessStation,
    }
    _workbench_specs: Dict[str, Type[Workbench]] = {
        "loop": LoopWorkbench,
    }

    @classmethod
    def _add_spec(
        cls,
        group: Literal["station", "workbench"],
        name: str,
        type_: Union[
            Type[Station], Creator[Station], Type[Workbench], Creator[Workbench]
        ],
    ):
        specs: Union[Dict[str, Type[Station]], Dict[str, Type[Workbench]]] = (
            cls._station_specs if group == "station" else cls._workbench_specs
        )
        if name in specs:
            raise ValueError(f"{group} name already exists: {name}")
        if isinstance(type_, Creator):
            specs[name] = type_.type  # type: ignore
        else:
            specs[name] = type_  # type: ignore

    @classmethod
    def _remove_spec(cls, group: Literal["station", "workbench"], name: str):
        specs: Union[Dict[str, Type[Station]], Dict[str, Type[Workbench]]] = (
            cls._station_specs if group == "station" else cls._workbench_specs
        )
        specs.pop(name, None)

    @classmethod
    def _get_spec(
        cls,
        group: Literal["station", "workbench"],
        name: str,
    ) -> Creator:
        specs: Union[Dict[str, Type[Station]], Dict[str, Type[Workbench]]] = (
            cls._station_specs if group == "station" else cls._workbench_specs
        )
        return InstanceCreator(specs[name])

    @classmethod
    def add_station_spec(
        cls,
        name: str,
        station: Union[Type[Station], Creator[Station]],
    ):
        cls._add_spec("station", name, station)

    @classmethod
    def remove_station_spec(cls, name: str):
        cls._remove_spec("station", name)

    @classmethod
    def add_workbench_spec(
        cls,
        name: str,
        workbench: Union[Type[Workbench], Creator[Workbench]],
    ):
        cls._add_spec("workbench", name, workbench)

    @classmethod
    def remove_workbench_spec(cls, name: str):
        cls._remove_spec("workbench", name)


workshop_registry = WorkshopRegistry()


class ScopedWorkshopRegistry(WorkshopRegistry):
    def __init__(self):
        super().__init__()
        self._scoped_station_specs: Dict[str, Type[Station]] = {}
        self._excluded_station_names: Set[str] = set()
        self._scoped_workbench_specs: Dict[str, Type[Workbench]] = {}
        self._excluded_workbench_names: Set[str] = set()

    @override
    def _add_spec(  # type: ignore[override]
        self,
        group: Literal["station", "workbench"],
        name: str,
        station: Union[
            Type[Station], Creator[Station], Type[Workbench], Creator[Workbench]
        ],
    ):
        scoped_specs: Union[Dict[str, Type[Station]], Dict[str, Type[Workbench]]] = (
            self._scoped_station_specs
            if group == "station"
            else self._scoped_workbench_specs
        )
        if name in scoped_specs:
            raise ValueError(f"{group} name already exists: {name}")
        if isinstance(station, Creator):
            scoped_specs[name] = station.type  # type: ignore
        else:
            scoped_specs[name] = station  # type: ignore
        excluded_names: Set[str] = (
            self._excluded_station_names
            if group == "station"
            else self._excluded_workbench_names
        )
        if name in excluded_names:
            excluded_names.remove(name)

    @override
    def _remove_spec(  # type: ignore[override]
        self,
        group: Literal["station", "workbench"],
        name: str,
    ):
        scoped_specs: Dict[str, Any]
        if group == "station":
            scoped_specs = self._scoped_station_specs
            excluded_names = self._excluded_station_names
        else:
            scoped_specs = self._scoped_workbench_specs
            excluded_names = self._excluded_workbench_names
        scoped_specs.pop(name, None)
        if name in excluded_names:
            excluded_names.remove(name)

    def get_station(self, name: str) -> Type[Station]:
        if name in self._excluded_station_names:
            raise ValueError(f"Station name is excluded: {name}")
        elif (value := self._scoped_station_specs.get(name)) is not None:
            return value
        elif (value := self._station_specs.get(name)) is not None:
            return value
        raise ValueError(f"Station name not found: {name}")

    def get_workbench(self, name: str) -> Type[Workbench]:
        if name in self._excluded_workbench_names:
            raise ValueError(f"Workbench name is excluded: {name}")
        elif (value := self._scoped_workbench_specs.get(name)) is not None:
            return value
        elif (value := self._workbench_specs.get(name)) is not None:
            return value
        raise ValueError(f"Workbench name not found: {name}")


class Workshop(ThreadStation):
    def __init__(self):
        super().__init__(
            workbench_creator=InstanceCreator(SupervisorWorkbench),
            worker_creator=InstanceCreator(Supervisor).bind(worker_specs=[]),
        )
        self._registry = ScopedWorkshopRegistry()

    def register(
        self,
        worker: Union[Type, Creator],
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

        worker_creator: Creator
        workbench_creator: Creator[Workbench]
        station_creator: Creator[Station]

        if isinstance(worker, InstanceCreator):
            worker_creator = worker
        elif issubclass(wk_t := cast(Type, worker), Type):
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

        if isinstance(station, InstanceCreator):
            kwargs = station.kwargs
            if "worker_creator" in kwargs:
                raise ValueError("Cannot specify worker_creator in station_creator")
            if "workbench_creator" in kwargs:
                raise ValueError("Cannot specify workbench_creator in station_creator")
            kwargs["worker_creator"] = worker_creator
            kwargs["workbench_creator"] = workbench_creator
            station_creator = station
        else:
            if station is None:
                station_creator = InstanceCreator(ThreadStation)
            elif isinstance(station, str):
                st_t = self._registry.get_station(station)
                station_creator = InstanceCreator(st_t)
            elif issubclass(st_t := cast(Type[Station], station), Station):
                station_creator = InstanceCreator(st_t)
            else:
                raise TypeError(f"Unexpected station type: {type(station)}")
            station_creator = station_creator.bind(
                workbench_creator=workbench_creator,
                worker_creator=worker_creator,
            )

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
