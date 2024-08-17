from abc import ABC, abstractmethod
from types import TracebackType

from typing_extensions import TYPE_CHECKING, Optional, Self, Type

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import Creator
    from flexplan.messages.mail import Mail
    from flexplan.workbench.base import Workbench
    from flexplan.workers.base import Worker


class StationSpec:
    __slots__ = ("use_process_future",)

    def __init__(
        self,
        *,
        use_process_future: bool,
    ):
        self.use_process_future = use_process_future


class Station(ABC):
    def __init__(
        self,
        *,
        workbench_creator: "Creator[Workbench]",
        worker_creator: "Creator[Worker]",
    ):
        self._workbench_creator = workbench_creator
        self._worker_creator = worker_creator
        self._worker_class = worker_creator.type

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    def __enter__(self) -> Self:
        if not self.is_running():
            self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.stop()

    @abstractmethod
    def is_running(self) -> bool:
        ...

    @abstractmethod
    def send(self, mail: "Mail") -> None:
        ...

    @abstractmethod
    def recv(self, timeout: Optional[float] = None) -> "Optional[Mail]":
        ...

    @property
    def worker_class(self) -> "Type[Worker]":
        return self._worker_class

    @property
    @abstractmethod
    def spec(self) -> StationSpec:
        ...
