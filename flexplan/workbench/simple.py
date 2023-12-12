from queue import Empty

from flexplan.workbench.base import AnyQueue, Workbench


class SimpleWorkbench(Workbench):
    def run(
        self,
        *,
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
