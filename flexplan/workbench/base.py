from abc import ABC, abstractmethod
from sys import _getframe as get_frame
from weakref import ref

from typing_extensions import TYPE_CHECKING, Any, Optional, Self, Type

from flexplan.utils.inspect import get_method_class

if TYPE_CHECKING:
    from weakref import ReferenceType

    from flexplan.datastructures.instancecreator import Creator
    from flexplan.datastructures.types import EventLike, TracebackType
    from flexplan.messages.mail import Mail, MailBox
    from flexplan.workers.base import Worker


class WorkbenchContext:
    def __init__(self, *, worker: "Worker", outbox: "MailBox") -> None:
        self.worker_ref: "ReferenceType[Worker]" = ref(worker)
        self.outbox_ref: "ReferenceType[MailBox]" = ref(outbox)
        self.worker_cls = type(worker)

    def post_init_worker(self) -> None:
        worker = self.worker_ref()
        if not worker or not hasattr(worker, "__post_init__"):
            return
        worker.__post_init__()

    def handle(self, mail: "Mail") -> Any:
        try:
            instruction = mail.instruction
            if isinstance(instruction, str):
                raise NotImplementedError()
            elif callable(instruction):
                cls = get_method_class(instruction)
                if cls is not self.worker_cls:
                    raise ValueError(
                        f"{instruction!r} is not a method of {self.worker_cls!r}"
                    )
                result = instruction(cls, *mail.args, **mail.kwargs)
                if mail.future:
                    mail.future.set_result(result)
                return result
            else:
                raise ValueError(f"{instruction!r} is not callable")
        except Exception as exc:
            if mail.future:
                mail.future.set_exception(exc)
        finally:
            del self, mail

    @classmethod
    def get_context(cls, depth: int = 2) -> Optional[Self]:
        try:
            frame = get_frame(depth)
        except ValueError:
            return None
        while True:
            co_self = frame.f_locals.get("self")
            co_name = frame.f_code.co_name
            print(f"{co_self=}, {co_name=}")
            if isinstance(co_self, cls) and co_name == cls.handle.__name__:
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
        worker_creator: "Creator[Worker]",
        inbox: "MailBox",
        outbox: "MailBox",
        running_event: "Optional[EventLike]" = None,
        terminate_event: "Optional[EventLike]" = None,
        **kwargs,
    ) -> None:
        ...


class _EnterWorkerContext:
    __slots__ = ("worker",)

    def __init__(self, worker: "Worker") -> None:
        self.worker = worker

    def __enter__(self) -> "Worker":
        if hasattr(self.worker, "__enter__"):
            self.worker.__enter__()
        return self.worker

    def __exit__(
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_val: "Optional[BaseException]",
        exc_tb: "Optional[TracebackType]",
    ) -> None:
        if hasattr(self.worker, "__exit__"):
            self.worker.__exit__(exc_type, exc_val, exc_tb)


enter_worker_context = _EnterWorkerContext
