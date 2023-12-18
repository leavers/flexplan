from abc import ABC, abstractmethod
from sys import _getframe as get_frame
from weakref import proxy

from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Optional,
    Self,
    final,
)

if TYPE_CHECKING:
    from weakref import ProxyType

    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import EventLike, QueueLike
    from flexplan.messages.mail import Mail
    from flexplan.workers.base import Worker


@final
class WorkbenchContext:
    def __init__(self, *, worker: Worker, outbox: QueueLike) -> None:
        self.worker_proxy: ProxyType[Worker] = proxy(worker)
        self.outbox_proxy: ProxyType[QueueLike] = proxy(outbox)

    def process(self, mail: Mail) -> Any:
        try:
            func = mail.message
            args = mail
            result = func(*args, **kwargs)
        except Exception as e:
            result = e
        return result

    @classmethod
    def search_context(cls, depth: int = 2) -> Optional[Self]:
        try:
            frame = get_frame(depth)
        except ValueError:
            return None
        while True:
            co_self = frame.f_locals.get("self")
            co_name = frame.f_code.co_name
            if isinstance(co_self, cls) and co_name == "invoke":
                return co_self
            if not frame.f_back:
                break
            frame = frame.f_back
        return None


class Workbench(ABC):
    @abstractmethod
    def run(
        self,
        *,
        worker_creator: InstanceCreator[Worker],
        inbox: QueueLike,
        outbox: QueueLike,
        running_event: Optional[EventLike] = None,
        terminate_event: Optional[EventLike] = None,
        **kwargs,
    ) -> None:
        ...
