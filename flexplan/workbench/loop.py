from queue import Empty

import typing_extensions as t

from flexplan.workbench.base import Workbench, WorkbenchContext


if t.TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import QueueLike
    from flexplan.workers.base import Worker


class LoopWorkbench(Workbench):
    def run(
        self,
        *,
        worker_creator: "InstanceCreator[Worker]",
        inbox: "QueueLike",
        outbox: "QueueLike",
        **kwargs,
    ) -> None:
        worker = worker_creator.create()
        context = WorkbenchContext(worker=worker, outbox=outbox)

        while True:
            try:
                mail = inbox.get(timeout=1)
            except Empty:
                continue
            if mail is None:
                break
            context.invoke(mail)
        while not inbox.empty():
            mail = inbox.get()
            if mail is None:
                continue
            context.invoke(mail)


class ConcurrentLoopWorkbench(Workbench):
    def run(
        self,
        *,
        worker_creator: InstanceCreator["Worker"],
        inbox: QueueLike,
        outbox: QueueLike,
        **kwargs,
    ) -> None:
        ...
