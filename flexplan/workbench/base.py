import multiprocessing as mp
from abc import ABC, abstractmethod
from queue import Queue

import typing_extensions as t

from flexplan.datastructures.instancecreator import InstanceCreator

if t.TYPE_CHECKING:
    from flexplan.workers.base import Worker

AnyQueue = t.Union[Queue, mp.Queue]


class Workbench(ABC):
    @abstractmethod
    def run(
        self,
        *,
        worker_creator: InstanceCreator["Worker"],
        inbox: AnyQueue,
        outbox: AnyQueue,
        **kwargs,
    ) -> None:
        ...