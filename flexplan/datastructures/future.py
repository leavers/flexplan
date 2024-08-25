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

        def wrapped(self: "FutureProxy", result: Any):  # type: ignore
            self._future.set_result(_pickle.dumps(result))
            self._invoke_callbacks()  # type: ignore

    elif method_name == "result":

        def wrapped(  # type: ignore
            self: "FutureProxy",
            timeout: Optional[float] = None,
        ):
            try:
                if self._simple:
                    return self._future.result(timeout=timeout)
                else:
                    raw = self._future.result(timeout=timeout)
                    return _pickle.loads(raw)
            finally:
                self = None  # type: ignore

    elif invoke_callback:

        def wrapped(self: "FutureProxy", *args, **kwargs):  # type: ignore
            res = getattr(self._future, method_name)(*args, **kwargs)
            self._invoke_callbacks()  # type: ignore
            return res

    else:

        def wrapped(self: "FutureProxy", *args, **kwargs):  # type: ignore
            return getattr(self._future, method_name)(*args, **kwargs)

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


class FutureProxyMeta(type):
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


class FutureProxy(Future, metaclass=FutureProxyMeta):
    def __init__(self, future: BuiltinFuture) -> None:
        self._future = future
        self._simple = isinstance(future, BuiltinFuture)
        self._done_callbacks: List[Callable[["FutureProxy"], Any]] = []

    def __repr__(self) -> str:
        # reimplement to avoid calling self._condition and self._state
        state = self.get_state()
        if state == FINISHED:
            try:
                res = self.result()
                return (
                    f"<{self.__class__.__name__} at "
                    f"{id(self):#x} state={_STATE_TO_DESCRIPTION_MAP[state]} "
                    f"returned {res.__class__.__name__}>"
                )
            except Exception as exc:
                return (
                    f"<{self.__class__.__name__} at "
                    f"{id(self):#x} state={_STATE_TO_DESCRIPTION_MAP[state]} "
                    f"raised {exc.__class__.__name__}>"
                )
        return (
            f"<{self.__class__.__name__} at "
            f"{id(self):#x} state={_STATE_TO_DESCRIPTION_MAP[state]} "
        )

    def add_done_callback(self, fn):
        if not self.done():
            self._done_callbacks.append(fn)
            return
        try:
            fn(self)
        except Exception:
            print("exception calling callback for %r", self)

    def unwrap(self) -> Future:
        return self._future


class ProcessFutureManager(SyncManager):
    def Future(self) -> FutureProxy:
        raise NotImplementedError()


ProcessFutureManager.register("Future", Future)
