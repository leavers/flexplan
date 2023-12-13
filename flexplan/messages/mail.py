from typing_extensions import (
    TYPE_CHECKING,
    Callable,
    FrozenSet,
    List,
    Optional,
    Type,
    final,
)

if TYPE_CHECKING:
    from concurrent.futures import Future

    from flexplan.datastructures.uniqueid import UniqueID
    from flexplan.messages.message import Message


@final
class ContactInfo:
    __slots__ = ("worker_cls", "tags", "actor_id")

    def __init__(
        self,
        worker_cls: Type,
        tags: FrozenSet[str],
        actor_id: Optional["UniqueID"] = None,
    ) -> None:
        self.worker_cls = worker_cls
        self.tags = tags
        self.actor_id = actor_id

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} at {hex(id(self))} "
            f"actor_cls={self.worker_cls} tags={self.tags} actor_id={self.actor_id}>"
        )


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
    __slots__ = ("source", "target", "trace")

    def __init__(
        self,
        *,
        source: ContactInfo,
        target: ContactInfo,
        trace: Optional[List[MailTrace]] = None,
    ):
        self.source = source
        self.target = target
        self.trace = trace or []


@final
class Mail:
    __slots__ = ("message", "future", "extra")

    def __init__(
        self,
        message: "Message",
        future: Optional["Future"] = None,
        extra: Optional[MailMeta] = None,
    ) -> None:
        self.message = message
        self.future = future
        self.extra = extra
