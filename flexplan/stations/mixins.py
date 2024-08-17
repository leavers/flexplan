from abc import abstractmethod

from typing_extensions import Optional


class RuntimeInfo:
    __slots__ = ("process_future_manager_address",)

    def __init__(
        self,
        *,
        process_future_manager_address: Optional[str] = None,
    ):
        self.process_future_manager_address = process_future_manager_address


class NotifyRuntimeInfoMixin:
    @abstractmethod
    def notify_runtime_info(self, info: RuntimeInfo) -> None:
        ...
