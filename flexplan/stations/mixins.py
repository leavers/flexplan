from abc import abstractmethod
from dataclasses import dataclass

from typing_extensions import Optional


@dataclass
class RuntimeInfo:
    process_future_manager_address: Optional[str] = None


class NotifyRuntimeInfoMixin:
    @abstractmethod
    def notify_runtime_info(self, info: RuntimeInfo) -> None:
        ...
