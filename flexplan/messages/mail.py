from typing_extensions import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Self,
    Sequence,
    Union,
    final,
)

from flexplan.datastructures.deferredbox import DeferredBox
from flexplan.datastructures.future import Future
from flexplan.datastructures.types import QueueLike
from flexplan.messages.message import Message


@final
class ContactInfo:
    # TODO
    ...


@final
class MailTrace:
    __slots__ = ("contact_info", "invocation")

    def __init__(
        self,
        *,
        contact_info: ContactInfo,
        invocation: Callable,
    ):
        self.contact_info = contact_info
        self.invocation = invocation


@final
class MailMeta:
    __slots__ = ("sender", "receivers", "trace")

    def __init__(
        self,
        *,
        sender: ContactInfo,
        receivers: List[ContactInfo],
        trace: Optional[List[MailTrace]] = None,
    ):
        self.sender = sender
        self.receivers = receivers
        self.trace = trace or []


@final
class Mail:
    __slots__ = ("instruction", "args", "kwargs", "future", "meta")

    def __init__(
        self,
        instruction,
        *,
        args: Sequence[Any] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        meta: MailMeta,
        future: Optional[Union[DeferredBox, Future]] = None,
    ) -> None:
        self.instruction = instruction
        self.args = tuple(args)
        if kwargs is None:
            kwargs = {}
        self.kwargs = dict(kwargs)
        self.future = future
        self.meta = meta

    @classmethod
    def new(
        cls,
        *,
        message: Message,
        future: Optional[Union[DeferredBox, Future]] = None,
    ) -> Self:
        args = message.args
        if args is None:
            args = ()
        kwargs = message.kwargs
        if kwargs is None:
            kwargs = {}

        return cls(
            instruction=message.instruction,
            args=args,
            kwargs=kwargs,
            # TODO
            meta=MailMeta(
                sender=ContactInfo(),
                receivers=[ContactInfo()],
                trace=[],
            ),
            future=future,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.instruction.__qualname__}, args={self.args!r}, kwargs={self.kwargs!r})"
        )


MailOrError = Optional[Union[Mail, BaseException]]
MailBox = QueueLike[MailOrError]
