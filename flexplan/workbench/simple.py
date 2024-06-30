from queue import Empty

from typing_extensions import TYPE_CHECKING

from flexplan.workbench.base import Workbench

if TYPE_CHECKING:
    from flexplan.messages.mail import MailBox


class SimpleWorkbench(Workbench):
    def run(
        self,
        *,
        inbox: "MailBox",
        outbox: "MailBox",
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
