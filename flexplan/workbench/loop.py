from queue import Empty

from typing_extensions import TYPE_CHECKING, Optional, override

from flexplan.workbench.base import Workbench, WorkbenchContext, enter_worker_context

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import Creator
    from flexplan.datastructures.types import EventLike
    from flexplan.messages.mail import MailBox
    from flexplan.stations.base import StationSpec
    from flexplan.workers.base import Worker


class LoopWorkbench(Workbench):
    @override
    def run(
        self,
        *,
        station_spec: "StationSpec",
        worker_creator: "Creator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        process_future_manager_address: Optional[str] = None,
        **kwargs,
    ) -> None:
        print(LoopWorkbench)
        worker = worker_creator.create()
        context = WorkbenchContext(
            station_spec=station_spec,
            worker=worker,
            outbox=outbox,
            process_future_manager_address=process_future_manager_address,
        )

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
        station_spec: "StationSpec",
        worker_creator: "Creator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        process_future_manager_address: Optional[str] = None,
        **kwargs,
    ) -> None:
        print(ConcurrentLoopWorkbench)
        worker = worker_creator.create()
        context = WorkbenchContext(
            station_spec=station_spec,
            worker=worker,
            outbox=outbox,
            process_future_manager_address=process_future_manager_address,
        )

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
