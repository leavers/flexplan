from multiprocessing import get_context
from queue import Empty

from typing_extensions import TYPE_CHECKING, Optional, Union, override

from flexplan.datastructures.instancecreator import Creator
from flexplan.messages.mail import Mail
from flexplan.stations.base import Station, StationSpec
from flexplan.stations.mixins import NotifyRuntimeInfoMixin, RuntimeInfo
from flexplan.workbench.base import Workbench
from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
    from multiprocessing.process import BaseProcess

    AnyContext = Union[ForkContext, ForkServerContext, SpawnContext]


class ProcessStation(Station, NotifyRuntimeInfoMixin):
    def __init__(
        self,
        *,
        workbench_creator: Creator[Workbench],
        worker_creator: Creator[Worker],
        mp_context: "Optional[AnyContext]" = None,
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
        )
        mp_ctx = get_context("spawn") if mp_context is None else mp_context
        self._mp_ctx = mp_ctx
        self._inbox = mp_ctx.Queue()
        self._outbox = mp_ctx.Queue()
        self._invoked: bool = False
        self._running_event = mp_ctx.Event()
        self._terminate_event = mp_ctx.Event()
        self._process: "Optional[BaseProcess]" = None
        self._process_future_manager_address: Optional[str] = None
        self._spec = StationSpec(use_process_future=True)

    @override
    def notify_runtime_info(self, info: RuntimeInfo) -> None:
        try:
            if info.process_future_manager_address is None:
                raise ValueError("process_future_manager_address is None")
            self._process_future_manager_address = info.process_future_manager_address
        except Exception as e:
            print(e)
            raise

    @override
    def start(self):
        if self.is_running():
            raise RuntimeError(f"{self.__class__.__name__} is already running")
        elif self._process_future_manager_address is None:
            raise ValueError("process_future_manager_address is None")
        self._invoked = True
        workbench = self._workbench_creator.create()
        self._process = self._mp_ctx.Process(
            target=workbench.run,
            kwargs={
                "station_spec": self._spec,
                "worker_creator": self._worker_creator,
                "inbox": self._inbox,
                "outbox": self._outbox,
                "running_event": self._running_event,
                "terminate_event": self._terminate_event,
                "process_future_manager_address": self._process_future_manager_address,
            },
            daemon=True,
        )
        self._process.start()

        re = self._running_event
        outbox = self._outbox
        while not re.is_set():
            try:
                exc = outbox.get(timeout=0.05)
            except Empty:
                continue
            if isinstance(exc, BaseException):
                raise exc

    @override
    def stop(self):
        if not self._invoked or self._process is None:
            return
        self._inbox.put(None)
        self._process.join()
        self._process = None

    @override
    def is_running(self) -> bool:
        return self._running_event.is_set()

    @override
    def send(self, mail: Mail) -> None:
        self._inbox.put(mail)

    @override
    def recv(self, timeout: Optional[float] = None) -> Optional[Mail]:
        try:
            return self._outbox.get(timeout=timeout)
        except Empty:
            return None

    @property
    @override
    def spec(self) -> StationSpec:
        return self._spec


class ForkProcessStation(ProcessStation):
    def __init__(
        self,
        *,
        workbench_creator: Creator[Workbench],
        worker_creator: Creator[Worker],
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
            mp_context=get_context("fork"),
        )


class ForkServerProcessStation(ProcessStation):
    def __init__(
        self,
        *,
        workbench_creator: Creator[Workbench],
        worker_creator: Creator[Worker],
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
            mp_context=get_context("forkserver"),
        )


class SpawnProcessStation(ProcessStation):
    def __init__(
        self,
        *,
        workbench_creator: Creator[Workbench],
        worker_creator: Creator[Worker],
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
            mp_context=get_context("spawn"),
        )
