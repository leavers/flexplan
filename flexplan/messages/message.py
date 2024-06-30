from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Self,
    Tuple,
    final,
)

if TYPE_CHECKING:
    from flexplan.datastructures.future import Future


@final
class MessageMeta:
    def __init__(self):
        self.receivers: List[Tuple[Any, bool]] = []


@final
class Message:
    __slots__ = ("instruction", "args", "kwargs", "meta")

    def __init__(self, instruction: Callable):
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

    def params(self, *args, **kwargs) -> Self:
        if self.args is not None or self.kwargs is not None:
            raise RuntimeError("Params already set")
        self.args = args
        self.kwargs = kwargs
        return self

    def submit(self) -> "Future":
        return self._send(use_future=True)  # type: ignore[return-value]

    def dispatch(self) -> None:
        self._send(use_future=False)

    def _send(self, use_future: bool) -> "Optional[Future]":
        from flexplan.datastructures.future import Future
        from flexplan.messages.mail import Mail
        from flexplan.workbench.base import WorkbenchContext

        context = WorkbenchContext.get_context(2)
        if context is None:
            raise RuntimeError("Message should be sent from a running Worker")
        outbox = context.outbox_ref()
        if outbox is None:
            raise RuntimeError("Worker context is corrupted")

        if use_future:
            future: Optional[Future] = Future()
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
