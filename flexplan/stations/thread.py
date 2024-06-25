from queue import Empty, Queue
from threading import Event, Thread

from typing_extensions import TYPE_CHECKING, Optional, override

from flexplan.stations.base import Station

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import Creator, InstanceCreator
    from flexplan.messages.mail import Mail
    from flexplan.workbench.base import Workbench
    from flexplan.workers.base import Worker


class ThreadStation(Station):
    def __init__(
        self,
        *,
        workbench_creator: "Creator[Workbench]",
        worker_creator: "Creator[Worker]",
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
        )
        self._inbox: Queue = Queue()
        self._outbox: Queue = Queue()
        self._invoked: bool = False
        self._running_event = Event()
        self._terminate_event = Event()
        self._thread: Optional[Thread] = None

    @override
    def start(self):
        if self.is_running():
            raise RuntimeError(f"{self.__class__.__name__} is already running")
        self._invoked = True
        workbench = self._workbench_creator.create()
        self._thread = Thread(
            target=workbench.run,
            kwargs={
                "worker_creator": self._worker_creator,
                "inbox": self._inbox,
                "outbox": self._outbox,
                "running_event": self._running_event,
                "terminate_event": self._terminate_event,
            },
            daemon=True,
        )
        self._thread.start()

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
        if not self._invoked or self._thread is None:
            return
        self._inbox.put(None)
        self._thread.join()
        self._thread = None

    @override
    def is_running(self) -> bool:
        return self._running_event.is_set()

    @override
    def send(self, mail: "Mail") -> None:
        self._inbox.put(mail)
