from typing_extensions import TYPE_CHECKING

from flexplan.workers.base import Worker

if TYPE_CHECKING:
    from flexplan.workers.base import Worker


class Supervisor(Worker):
    def __init__(self):
        super().__init__()
