from abc import ABC, abstractmethod


class Workbench(ABC):
    @abstractmethod
    def run(self, *arg, **kwargs):
        ...