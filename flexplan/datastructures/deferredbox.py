from threading import Event
from typing import Generic, Optional, TypeVar, cast

T = TypeVar("T")


class DeferredBox(Generic[T]):
    def __init__(self):
        self._value: Optional[T] = None
        self._set_event = Event()

    def set(self, value: T):
        self._value = value
        self._set_event.set()

    def get(self) -> T:
        self._set_event.wait()
        return cast(T, self._value)
