from queue import Queue

import typing_extensions as t

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.stations.base import Station

if t.TYPE_CHECKING:
    from flexplan.workbench.base import Workbench
    from flexplan.workers.base import Worker


class ThreadStation(Station):
    """A station that runs a thread."""

    def __init__(
        self,
        *,
        workbench_creator: InstanceCreator["Workbench"],
        worker_creator: InstanceCreator["Worker"],
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
        )
        self._inbox = Queue()
        self._outbox = Queue()

    def run(self):
        """Run the thread."""
        self.thread.start()
        self.thread.join()
