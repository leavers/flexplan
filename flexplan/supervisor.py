from queue import Empty
from types import TracebackType

from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    cast,
    override,
)

from flexplan.messages.mail import Mail
from flexplan.utils.inspect import getmethodclass
from flexplan.workbench.base import Workbench, WorkbenchContext, enter_worker_context
from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import EventLike
    from flexplan.errors import WorkerNotFoundError, WorkerRuntimeError
    from flexplan.messages.mail import MailBox
    from flexplan.stations.base import Station
    from flexplan.types import StationSpec


class Supervisor(Worker):
    def __init__(
        self,
        station_specs: "Optional[List[StationSpec]]" = None,
    ):
        super().__init__()
        stations_preparation: Dict[str, "InstanceCreator[Station]"] = {}
        if station_specs:
            for name, station_creator in station_specs:
                if name is None:
                    name = f"station_{len(stations_preparation)}"
                elif not isinstance(name, str):
                    raise TypeError(f"Unexpected name type: {type(name)}")
                elif name in stations_preparation:
                    raise ValueError(f"Duplicate station name: {name}")
                stations_preparation[name] = station_creator
        self._station_preparation = stations_preparation
        self._worker_stations: Dict[str, "Station"] = {}

    def __post_init__(self):
        worker_stations = self._worker_stations
        for name, station_creator in self._station_preparation.items():
            print(f"Create station {name!r}: {station_creator}")
            station = station_creator.create()
            station.start()
            worker_stations[name] = station

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        for name, station in self._worker_stations.items():
            print(f"Stop station {name!r}: {station}")
            station.stop()

    def handle(self, mail: Mail):
        instruction = mail.instruction
        if isinstance(instruction, str):
            raise NotImplementedError()
        elif not callable(instruction):
            raise ValueError(f"{instruction!r} is not callable")

        try:
            cls = getmethodclass(instruction)
            if cls is None:
                # simple function
                raise NotImplementedError()
            elif cls is type(self):
                result = instruction(self, *mail.args, **mail.kwargs)
                if mail.future:
                    mail.future.set_result(result)
            else:
                instance: Any = None
                for station in self._worker_stations.values():
                    print(f"{cls=!r} in {station.worker_class=}")
                    if cls is station.worker_class:
                        instance = cls
                        break
                if instance is None:
                    raise WorkerNotFoundError(f"{cls!r} is not available")
                result = instruction(instance, *mail.args, **mail.kwargs)
                if mail.future:
                    mail.future.set_result(result)
        except BaseException as exc:
            if mail.future:
                mail.future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise


class SupervisorContext(WorkbenchContext):
    @override
    def handle(self, mail: Mail) -> Any:
        try:
            if supervisor := cast(Optional[Supervisor], self.worker_ref()):
                supervisor.handle(mail)
            else:
                raise WorkerRuntimeError(
                    f"Supervisor {self.worker_cls!r} is not available"
                )
        except BaseException as exc:
            if mail.future:
                mail.future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise
        finally:
            del self, mail


class SupervisorWorkbench(Workbench):
    @override
    def run(
        self,
        *,
        worker_creator: "InstanceCreator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        **kwargs,
    ) -> None:
        worker = worker_creator.create()
        context = SupervisorContext(worker=worker, outbox=outbox)

        def is_running() -> bool:
            if running_event is None:
                return True
            return running_event.is_set()

        if running_event is not None:
            running_event.set()

        context.post_init_worker()

        with enter_worker_context(worker):
            while is_running():
                try:
                    mail = inbox.get(timeout=0.1)
                except Empty:
                    continue
                if mail is None:
                    break
                context.handle(mail)
            while not inbox.empty():
                mail = inbox.get()
                if mail is None:
                    continue
                context.handle(mail)

        if running_event is not None:
            running_event.clear()
