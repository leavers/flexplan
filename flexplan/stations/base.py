from abc import ABC, abstractmethod


class Station(ABC):
    def __init__(
        self,
        workbench_spec,
        worker_spec,
    ):
        self.workbench_spec = workbench_spec
        self.worker_spec = worker_spec

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def stop(self) -> None:
        ...
