from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.stations.thread import ThreadStation
from flexplan.workbench.loop import ConcurrentLoopWorkbench
from flexplan.workers.supervisor import Supervisor


class Workshop(ThreadStation):
    def __init__(self):
        super().__init__(
            workbench_creator=InstanceCreator(ConcurrentLoopWorkbench),
            worker_creator=InstanceCreator(Supervisor),
        )
