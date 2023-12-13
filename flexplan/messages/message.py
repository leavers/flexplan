from typing_extensions import Callable, Iterable, Self, final

from flexplan.datastructures.future import Future


@final
class Message:
    def __init__(self, invocation: Callable):
        self._invocation = invocation

    def to(self, tags: Iterable[str]) -> Self:
        return self

    def params(self, *args, **kwargs) -> Self:
        return self

    def submit(self) -> Future:
        from flexplan.messages.mail import Mail

    def dispatch(self) -> None:
        ...
