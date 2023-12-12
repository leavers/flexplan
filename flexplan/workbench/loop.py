from queue import Empty

import typing_extensions as t

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.workbench.base import AnyQueue, Workbench


if t.TYPE_CHECKING:
    from flexplan.workers.base import Worker

class LoopWorkbench(Workbench):
    def run(
        self,
        *,
        worker_creator: InstanceCreator["Worker"],
        inbox: AnyQueue,
        outbox: AnyQueue,
        **kwargs,
    ) -> None:
        while True:
            try:
                message = inbox.get(timeout=1)
            except Empty:
                continue
            if message is None:
                break
            outbox.put(message)
