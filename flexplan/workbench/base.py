import weakref
from abc import ABC, abstractmethod
from sys import _getframe as get_frame

from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Optional,
    Self,
    final,
)

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.datastructures.types import QueueLike
    from flexplan.messages.mail import Mail
    from flexplan.workers.base import Worker


@final
class WorkbenchContext:
    def __init__(self, *, worker: "Worker", outbox: "QueueLike") -> None:
        self._worker_proxy = weakref.proxy(worker)
        self._outbox_proxy = weakref.proxy(outbox)

    def invoke(self, mail: "Mail") -> Any:
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            result = e
        return result

    @classmethod
    def search_context(cls, depth: int = 1) -> Optional[Self]:
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
        worker_creator: "InstanceCreator[Worker]",
        inbox: "QueueLike",
        outbox: "QueueLike",
        **kwargs,
    ) -> None:
        ...
