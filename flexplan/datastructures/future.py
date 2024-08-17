from concurrent.futures import Future as BuiltinFuture
from concurrent.futures._base import _STATE_TO_DESCRIPTION_MAP, FINISHED
from multiprocessing.managers import SyncManager

from typing_extensions import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

T = TypeVar("T")


class Future(BuiltinFuture, Generic[T]):
    def result(self, timeout: Optional[float] = None) -> T:
        return super().result(timeout=timeout)

    def get_state(self) -> str:
        """Get future internal state.

        This is only for convenience to get the state when ``Future`` is registered in
        a :class:`multiprocessing.Manager`.
        """
        return self._state  # type: ignore


def _proxy_impl(
    method,
    method_name: str,
    class_name: str,
    module_name: str,
    invoke_callback: bool,
):
    from flexplan.utils.pickle import get_pickle

    _pickle = get_pickle()

    if method_name == "set_result":

        def wrapped(self: "ProcessFuture", result: Any):  # type: ignore
            self._remote.set_result(_pickle.dumps(result))
            self._invoke_callbacks()  # type: ignore

    elif method_name == "result":

        def wrapped(  # type: ignore
            self: "ProcessFuture",
            timeout: Optional[float] = None,
        ):
            try:
                raw = self._remote.result(timeout=timeout)
                return _pickle.loads(raw)
            finally:
                self = None  # type: ignore

    elif invoke_callback:

        def wrapped(self: "ProcessFuture", *args, **kwargs):  # type: ignore
            res = getattr(self._remote, method_name)(*args, **kwargs)
            self._invoke_callbacks()  # type: ignore
            return res

    else:

        def wrapped(self: "ProcessFuture", *args, **kwargs):  # type: ignore
            return getattr(self._remote, method_name)(*args, **kwargs)

    setattr(wrapped, "__module__", module_name)
    setattr(wrapped, "__name__", method_name)
    setattr(wrapped, "__qualname__", f"{class_name}.{method_name}")
    for attr in ("__doc__", "__annotations__"):
        try:
            value = getattr(method, attr)
        except AttributeError:
            pass
        else:
            setattr(wrapped, attr, value)
    getattr(wrapped, "__dict__").update(getattr(method, "__dict__", {}))
    return wrapped


class ProcessFutureMeta(type):
    def __new__(
        cls,
        name: str,
        bases: Tuple[Type, ...],
        namespace: Dict[str, Any],
        **kwargs,
    ):
        class_name = namespace["__qualname__"]
        module_name = namespace["__module__"]
        for attr_name, attr, invoke_callback in [
            ("cancel", Future.cancel, True),
            ("cancelled", Future.cancelled, False),
            ("running", Future.running, False),
            ("done", Future.done, False),
            ("result", Future.result, False),
            ("exception", Future.exception, False),
            ("get_state", Future.get_state, False),
            (
                "set_running_or_notify_cancel",
                Future.set_running_or_notify_cancel,
                False,
            ),
            ("set_result", Future.set_result, True),
            ("set_exception", Future.set_exception, True),
        ]:
            namespace[attr_name] = _proxy_impl(
                method=attr,
                method_name=attr_name,
                class_name=class_name,
                module_name=module_name,
                invoke_callback=invoke_callback,
            )

        return super().__new__(cls, name, bases, namespace, **kwargs)


class ProcessFuture(Future, metaclass=ProcessFutureMeta):
    def __init__(self, remote: Future) -> None:
        self._remote = remote
        self._done_callbacks: List[Callable[["ProcessFuture"], Any]] = []

    def __repr__(self) -> str:
        # reimplement to avoid calling self._condition and self._state
        state = self.get_state()
        if state == FINISHED:
            try:
                res = self.result()
                return "<%s at %#x state=%s returned %s>" % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[state],
                    res.__class__.__name__,
                )
            except Exception as exc:
                return "<%s at %#x state=%s raised %s>" % (
                    self.__class__.__name__,
                    id(self),
                    _STATE_TO_DESCRIPTION_MAP[state],
                    exc.__class__.__name__,
                )
        return "<%s at %#x state=%s>" % (
            self.__class__.__name__,
            id(self),
            _STATE_TO_DESCRIPTION_MAP[state],
        )

    def add_done_callback(self, fn):
        if not self.done():
            self._done_callbacks.append(fn)
            return
        try:
            fn(self)
        except Exception:
            print("exception calling callback for %r", self)


class ProcessFutureManager(SyncManager):
    def Future(self) -> ProcessFuture:
        raise NotImplementedError()


ProcessFutureManager.register("Future", Future)
