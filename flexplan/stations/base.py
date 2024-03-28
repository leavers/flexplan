from abc import ABC, abstractmethod
from types import TracebackType

from typing_extensions import TYPE_CHECKING, Optional, Self, Type

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.messages.mail import Mail
    from flexplan.workbench.base import Workbench
    from flexplan.workers.base import Worker


class Station(ABC):
    def __init__(
        self,
        *,
        workbench_creator: "InstanceCreator[Workbench]",
        worker_creator: "InstanceCreator[Worker]",
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

    @property
    def worker_class(self) -> "Type[Worker]":
        return self._worker_class
