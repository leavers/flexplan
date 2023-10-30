from abc import ABC
from concurrent.futures import Future


class Message(ABC):
    def __init__(self):
        ...

    def submit(self) -> Future:
        ...

    def dispatch(self) -> None:
        ...