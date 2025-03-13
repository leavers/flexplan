from typing_extensions import (
    Any,
    Callable,
    Concatenate,
    Dict,
    Generic,
    List,
    Optional,
    ParamSpec,
    Self,
    Tuple,
    TypeVar,
    final,
)

from flexplan.datastructures.future import Future

P = ParamSpec("P")
R = TypeVar("R")


@final
class MessageMeta:
    def __init__(self):
        self.receivers: List[Tuple[Any, bool]] = []


@final
class Message(Generic[P, R]):
    __slots__ = ("instruction", "args", "kwargs", "meta")

    def __init__(self, instruction: Callable[Concatenate[Any, P], R]):
        self.instruction = instruction
        self.args: Optional[Tuple[Any, ...]] = None
        self.kwargs: Optional[Dict[str, Any]] = None
        self.meta = MessageMeta()

    def to(
        self,
        receiver,
        *,
        notify_all: bool = False,
    ) -> Self:
        self.meta.receivers.append((receiver, notify_all))
        return self

    def params(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        if self.args is not None or self.kwargs is not None:
            raise RuntimeError("Params already set")
        self.args = args
        self.kwargs = kwargs
        return self

    def submit(self) -> Future[R]:
        return self._send(use_future=True)  # type: ignore[return-value]

    def emit(self) -> None:
        self._send(use_future=False)

    def _send(self, use_future: bool) -> "Optional[Future]":
        from flexplan.datastructures.future import Future
        from flexplan.messages.mail import Mail
        from flexplan.workbench.base import WorkbenchContext

        context = WorkbenchContext.get_context(2)
        if context is None:
            raise RuntimeError("Message should be sent from a running Worker")
        outbox = context._outbox_ref()
        if outbox is None:
            raise RuntimeError("Worker context is corrupted")

        if use_future:
            future: Optional[Future] = context.create_future()
        else:
            future = None
        mail = Mail.new(message=self, future=future)
        outbox.put(mail)
        return future

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.instruction.__name__} "
            f"args={self.args}, kwargs={self.kwargs})"
        )
