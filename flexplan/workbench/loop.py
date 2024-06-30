from queue import Empty

from typing_extensions import TYPE_CHECKING, Optional, override

from flexplan.workbench.base import Workbench, WorkbenchContext, enter_worker_context

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import EventLike
    from flexplan.messages.mail import MailBox
    from flexplan.workers.base import Worker


class LoopWorkbench(Workbench):
    @override
    def run(
        self,
        *,
        worker_creator: "InstanceCreator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        **kwargs,
    ) -> None:
        worker = worker_creator.create()
        context = WorkbenchContext(worker=worker, outbox=outbox)

        def is_running() -> bool:
            if running_event is None:
                return True
            return running_event.is_set()

        if running_event is not None:
            running_event.set()

        context.post_init_worker()

        with enter_worker_context(worker):
            while is_running():
                try:
                    mail = inbox.get(timeout=1)
                except Empty:
                    continue
                if mail is None:
                    break
                context.handle(mail)
            while not inbox.empty():
                mail = inbox.get()
                if mail is None:
                    continue
                context.handle(mail)

        if running_event is not None:
            running_event.clear()


class ConcurrentLoopWorkbench(Workbench):
    @override
    def run(
        self,
        *,
        worker_creator: "InstanceCreator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        **kwargs,
    ) -> None:
        worker = worker_creator.create()
        context = WorkbenchContext(worker=worker, outbox=outbox)

        def is_running() -> bool:
            if running_event is None:
                return True
            return running_event.is_set()

        if running_event is not None:
            running_event.set()

        context.post_init_worker()

        with enter_worker_context(worker):
            while is_running():
                try:
                    mail = inbox.get(timeout=1)
                except Empty:
                    continue
                if mail is None:
                    break
                context.handle(mail)
            while not inbox.empty():
                mail = inbox.get()
                if mail is None:
                    continue
                context.handle(mail)

        if running_event is not None:
            running_event.clear()
