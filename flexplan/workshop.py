from typing_extensions import TYPE_CHECKING

from flexplan.stations.thread import ThreadStation

if TYPE_CHECKING:
    from flexplan.datastructures.instancecreator import InstanceCreator
    from flexplan.workbench.base import Workbench
    from flexplan.workers.base import Worker


class Workshop(ThreadStation):
    def __init__(
        self,
        *,
        workbench_creator: InstanceCreator[Workbench],
        worker_creator: InstanceCreator[Worker],
    ):
        super().__init__(
            workbench_creator=workbench_creator,
            worker_creator=worker_creator,
        )
