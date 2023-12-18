from queue import Empty

from typing_extensions import TYPE_CHECKING, Optional

from flexplan.workbench.base import Workbench, WorkbenchContext


if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import EventLike, QueueLike
    from flexplan.workers.base import Worker


class LoopWorkbench(Workbench):
    def run(
        self,
        *,
        worker_creator: InstanceCreator[Worker],
        inbox: QueueLike,
        outbox: QueueLike,
        running_event: Optional[EventLike] = None,
        **kwargs,
    ) -> None:
        worker = worker_creator.create()
        context = WorkbenchContext(worker=worker, outbox=outbox)

        def is_running() -> bool:
            if running_event is None:
                return True
            return running_event.is_set()

        while is_running():
            try:
                mail = inbox.get(timeout=1)
            except Empty:
                continue
            if mail is None:
                break
            context.process(mail)
        while not inbox.empty():
            mail = inbox.get()
            if mail is None:
                continue
            context.process(mail)


class ConcurrentLoopWorkbench(Workbench):
    def run(
        self,
        *,
        worker_creator: InstanceCreator[Worker],
        inbox: QueueLike,
        outbox: QueueLike,
        **kwargs,
    ) -> None:
        ...
