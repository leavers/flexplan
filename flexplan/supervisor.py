from queue import Empty
from types import TracebackType
from weakref import ref

from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    cast,
    override,
)

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.errors import (
    ArgumentTypeError,
    ArgumentValueError,
    WorkerNotFoundError,
    WorkerRuntimeError,
)
from flexplan.messages.mail import Mail, MailBox
from flexplan.messages.message import Message
from flexplan.utils.inspect import get_method_class
from flexplan.workbench.base import Workbench, WorkbenchContext, enter_worker_context
from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import Creator
    from flexplan.datastructures.types import EventLike
    from flexplan.stations.base import Station
    from flexplan.types import WorkerSpec, WorkerId


class Supervisor(Worker):
    def __init__(
        self,
        worker_specs: "Optional[List[WorkerSpec]]" = None,
    ):
        super().__init__()
        _specs: "Dict[WorkerId, Tuple[Optional[str], Creator[Station]]]" = {}
        if worker_specs:
            for worker_id, name, station_creator in worker_specs:
                if not isinstance(worker_id, str):
                    raise ArgumentTypeError(
                        f"Unexpected worker_id type: {type(worker_id)}"
                    )
                elif worker_id in _specs:
                    raise ArgumentValueError(f"Duplicate station name: {name}")
                if name is None:
                    name = f"station_{len(_specs)}"
                elif not isinstance(name, str):
                    raise ArgumentTypeError(f"Unexpected name type: {type(name)}")
                if not isinstance(station_creator, InstanceCreator):
                    raise ArgumentTypeError(
                        f"Unexpected station creator type: {type(station_creator)}"
                    )
                _specs[worker_id] = (name, station_creator)
        self._specs = _specs
        self._worker_stations: "Dict[WorkerId, Station]" = {}

    def __post_init__(self):
        worker_stations = self._worker_stations
        if context := SupervisorContext.get_context():
            context.set_worker_stations(worker_stations)
        for worker_id, (name, station_creator) in self._specs.items():
            station = station_creator.create()
            station.start()
            worker_stations[worker_id] = station

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        for station in self._worker_stations.values():
            station.stop()

    def handle(self, mail: Mail):
        instruction = mail.instruction
        if isinstance(instruction, str):
            raise NotImplementedError("Preserved for string events/signals")
        elif not callable(instruction):
            raise ValueError(f"{instruction!r} is not callable")

        try:
            cls = get_method_class(instruction)
            if cls is None:
                raise NotImplementedError("Simple functions are not supported yet")
            elif cls is type(self):
                # supervisor method
                result = instruction(self, *mail.args, **mail.kwargs)
                if mail.future:
                    mail.future.set_result(result)
            else:
                station: Optional[Station] = None
                for worker_station in self._worker_stations.values():
                    if cls is worker_station.worker_class:
                        station = worker_station
                        break
                if station is None:
                    raise WorkerNotFoundError(f"Worker not found: {cls!r}")
                station.send(mail)
        except BaseException as exc:
            if mail.future:
                mail.future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise


class SupervisorContext(WorkbenchContext):
    def __init__(
        self,
        *,
        worker: "Supervisor",
        outbox: "MailBox",
        workbench: "SupervisorWorkbench",
    ) -> None:
        super().__init__(worker=worker, outbox=outbox)
        self._workbench = ref(workbench)

    def set_worker_stations(self, worker_stations: "Dict[WorkerId, Station]"):
        workbench = self._workbench()
        if workbench is None:
            raise WorkerRuntimeError(f"Workbench {self.worker_cls!r} is not available")
        workbench.set_worker_stations(worker_stations)

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
    def __init__(self):
        super().__init__()
        self._worker_stations: Optional[Dict[WorkerId, Station]] = None

    def set_worker_stations(self, worker_stations: "Dict[WorkerId, Station]"):
        self._worker_stations = worker_stations

    @override
    def run(
        self,
        *,
        worker_creator: "Creator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        **kwargs,
    ) -> None:
        try:
            supervisor = cast(Supervisor, worker_creator.create())
            context = SupervisorContext(
                worker=supervisor,
                outbox=outbox,
                workbench=self,
            )
        except BaseException as exc:
            outbox.put(exc)
            return

        def is_running() -> bool:
            if running_event is None:
                return True
            return running_event.is_set()

        if running_event is not None:
            running_event.set()

        if hasattr(supervisor, "__post_init__"):
            context.handle(
                Mail.new(message=Message(Supervisor.__post_init__).to(Supervisor))
            )

        with enter_worker_context(supervisor):
            while is_running():
                if self._worker_stations is not None:
                    for station in self._worker_stations.values():
                        if worker_mail := station.recv(0):
                            context.handle(worker_mail)
                try:
                    mail = inbox.get(timeout=0)
                except Empty:
                    continue
                if mail is None:
                    break
                elif isinstance(mail, BaseException):
                    raise WorkerRuntimeError() from mail
                context.handle(mail)
            while not inbox.empty():
                if self._worker_stations is not None:
                    for station in self._worker_stations.values():
                        if worker_mail := station.recv(0):
                            context.handle(worker_mail)
                mail = inbox.get()
                if mail is None:
                    continue
                elif isinstance(mail, BaseException):
                    raise WorkerRuntimeError() from mail
                context.handle(mail)
        self._worker_stations = None

        if running_event is not None:
            running_event.clear()
