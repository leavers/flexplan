from abc import abstractmethod, abstractproperty

import typing_extensions as t

if t.TYPE_CHECKING:
    from types import TracebackType

P = t.ParamSpec("P")
T = t.TypeVar("T")
T_co = t.TypeVar("T_co", covariant=True)


@t.runtime_checkable
class EventLike(t.Protocol):
    @abstractmethod
    def set(self) -> None:
        ...

    @abstractmethod
    def is_set(self) -> bool:
        ...

    @abstractmethod
    def clear(self) -> None:
        ...

    @abstractmethod
    def wait(self, timeout: t.Optional[float] = None) -> bool:
        ...


@t.runtime_checkable
class ConditionLike(t.Protocol):
    @abstractmethod
    def wait(self, timeout: t.Optional[float] = None) -> bool:
        ...

    @abstractmethod
    def wait_for(
        self,
        predicate: t.Callable[[], bool],
        timeout: t.Optional[float] = None,
    ) -> bool:
        ...

    @abstractmethod
    def notify(self, n: int = 1) -> None:
        ...

    @abstractmethod
    def notify_all(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> bool:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional["TracebackType"],
    ) -> None:
        ...


@t.runtime_checkable
class SemaphoreLike(t.Protocol):
    @abstractmethod
    def acquire(
        self,
        blocking: bool = True,
        timeout: t.Optional[float] = None,
    ) -> bool:
        ...

    @abstractmethod
    def release(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> bool:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional["TracebackType"],
    ) -> None:
        ...


@t.runtime_checkable
class LockLike(t.Protocol):
    @abstractmethod
    def acquire(self, *args, **kwargs) -> bool:
        ...

    @abstractmethod
    def release(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> bool:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None:
        ...


@t.runtime_checkable
class SimpleQueueLike(t.Protocol[T]):
    @abstractmethod
    def get(self) -> T:
        ...

    @abstractmethod
    def put(self, obj: T) -> None:
        ...

    @abstractmethod
    def empty(self) -> bool:
        ...


@t.runtime_checkable
class QueueLike(SimpleQueueLike, t.Protocol[T]):
    @abstractmethod
    def get(self, block: bool = ..., timeout: t.Optional[float] = ...) -> T:
        ...

    @abstractmethod
    def get_nowait(self) -> T:
        ...

    @abstractmethod
    def put(
        self,
        obj: T,
        block: bool = ...,
        timeout: t.Optional[float] = ...,
    ) -> None:
        ...

    @abstractmethod
    def put_nowait(self, obj: T) -> None:
        ...

    @abstractmethod
    def empty(self) -> bool:
        ...

    @abstractmethod
    def qsize(self) -> int:
        ...


@t.runtime_checkable
class ValueLike(t.Protocol[T]):
    @abstractproperty
    def value(self) -> T:
        ...

    @value.setter
    def value(self, val: T) -> None:
        ...


@t.runtime_checkable
class ParallelTask(t.Protocol):
    def __init__(
        self,
        target: t.Optional[t.Callable[..., t.Any]] = ...,
        args: t.Tuple[t.Any, ...] = (),
        kwargs: t.Optional[t.Dict[str, t.Any]] = ...,
        daemon: t.Optional[bool] = ...,
        **more,
    ) -> None:
        ...

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def join(self, timeout: t.Optional[float] = ...) -> None:
        ...

    @abstractmethod
    def is_alive(self) -> bool:
        ...


@t.runtime_checkable
class FutureLike(t.Protocol[T]):
    @abstractmethod
    def cancel(self) -> bool:
        ...

    @abstractmethod
    def cancelled(self) -> bool:
        ...

    @abstractmethod
    def running(self) -> bool:
        ...

    @abstractmethod
    def done(self) -> bool:
        ...

    @abstractmethod
    def add_done_callback(self, fn: t.Callable[[t.Self], t.Any]) -> None:
        ...

    @abstractmethod
    def result(self, timeout: t.Optional[float] = None) -> T:
        ...

    @abstractmethod
    def exception(self, timeout: t.Optional[float] = None) -> t.Optional[BaseException]:
        ...

    @abstractmethod
    def set_running_or_notify_cancel(self) -> bool:
        ...

    @abstractmethod
    def set_result(self, result: T) -> None:
        ...

    @abstractmethod
    def set_exception(self, exception: BaseException) -> None:
        ...


@t.runtime_checkable
class BoundMethodLike(t.Protocol[T]):
    __name__: str
    __self__: T
