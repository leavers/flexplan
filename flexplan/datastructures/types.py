from typing_extensions import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    ParamSpec,
    Protocol,
    Self,
    Tuple,
    Type,
    TypeVar,
)

if TYPE_CHECKING:
    from types import TracebackType

P = ParamSpec("P")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class EventLike(Protocol):
    def set(self) -> None: ...

    def is_set(self) -> bool: ...

    def clear(self) -> None: ...

    def wait(self, timeout: Optional[float] = None) -> bool: ...


class ConditionLike(Protocol):
    def wait(self, timeout: Optional[float] = None) -> bool: ...

    def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: Optional[float] = None,
    ) -> bool: ...

    def notify(self, n: int = 1) -> None: ...

    def notify_all(self) -> None: ...

    def __enter__(self) -> bool: ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: "Optional[TracebackType]",
    ) -> None: ...


class SemaphoreLike(Protocol):
    def acquire(
        self,
        blocking: bool = True,
        timeout: Optional[float] = None,
    ) -> bool: ...

    def release(self) -> None: ...

    def __enter__(self) -> bool: ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: "Optional[TracebackType]",
    ) -> None: ...


class LockLike(Protocol):
    def acquire(self, *args, **kwargs) -> bool: ...

    def release(self) -> None: ...

    def __enter__(self) -> bool: ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: "Optional[TracebackType]",
    ) -> None: ...


class SimpleQueueLike(Protocol[T]):
    def get(self) -> T: ...

    def put(self, obj: T) -> None: ...

    def empty(self) -> bool: ...


class QueueLike(SimpleQueueLike, Protocol[T]):
    def get(self, block: bool = ..., timeout: Optional[float] = ...) -> T: ...

    def get_nowait(self) -> T: ...

    def put(
        self,
        obj: T,
        block: bool = ...,
        timeout: Optional[float] = ...,
    ) -> None: ...

    def put_nowait(self, obj: T) -> None: ...

    def empty(self) -> bool: ...

    def qsize(self) -> int: ...


class ValueLike(Protocol[T]):
    @property
    def value(self) -> T: ...

    @value.setter
    def value(self, val: T) -> None: ...


class ParallelTask(Protocol):
    def __init__(
        self,
        target: Optional[Callable[..., Any]] = ...,
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = ...,
        daemon: Optional[bool] = ...,
        **more,
    ) -> None: ...

    def start(self) -> None: ...

    def join(self, timeout: Optional[float] = ...) -> None: ...

    def is_alive(self) -> bool: ...


class FutureLike(Protocol[T]):
    def cancel(self) -> bool: ...

    def cancelled(self) -> bool: ...

    def running(self) -> bool: ...

    def done(self) -> bool: ...

    def add_done_callback(self, fn: Callable[[Self], Any]) -> None: ...

    def result(self, timeout: Optional[float] = None) -> T: ...

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]: ...

    def set_running_or_notify_cancel(self) -> bool: ...

    def set_result(self, result: T) -> None: ...

    def set_exception(self, exception: BaseException) -> None: ...


class BoundMethodLike(Protocol[T]):
    __name__: str
    __self__: T


class PickleLike(Protocol):
    def loads(self, data: bytes) -> Any: ...

    def dumps(self, obj: Any) -> bytes: ...
