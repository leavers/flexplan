import typing_extensions as t

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.stations.base import Station
from flexplan.workbench.base import Workbench


class DummyQueue(list):
    def empty(self) -> bool:
        return len(self) == 0

    def get(self, *args, **kwargs) -> t.Any:
        return self.pop(0)

    def put(self, item: t.Any):
        self.append(item)

    def qsize(self) -> int:
        return len(self)


class LocalStation(Station):
    def __init__(
        self,
        *,
        workbench_creator: InstanceCreator[Workbench],
        worker_creator: InstanceCreator,
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
        )
        self._is_running = False
        self._inbox = DummyQueue()
        self._outbox = DummyQueue()

    def start(self) -> None:
        if self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is already running.")
        self._run()
        self._is_running = True

    def _run(self) -> None:
        workbench: Workbench = self._workbench_creator.create()
        workbench.run(inbox=self._inbox, outbox=self._outbox)

    def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
