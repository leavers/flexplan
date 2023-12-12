from abc import ABC, abstractmethod

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.messages.base import Message
from flexplan.workbench.base import Workbench
from flexplan.workers.base import Worker


class Station(ABC):
    def __init__(
        self,
        *,
        workbench_creator: InstanceCreator[Workbench],
        worker_creator: InstanceCreator[Worker],
    ):
        self._workbench_creator = workbench_creator
        self._worker_creator = worker_creator

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...

    @abstractmethod
    def is_running(self) -> bool:
        ...

    @abstractmethod
    def send(self) -> None:
        ...
