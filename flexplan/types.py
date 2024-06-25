from typing_extensions import Optional, Tuple

from flexplan.datastructures.instancecreator import Creator
from flexplan.stations.base import Station

# Don't construct WorkerId with NewType as it will not work with mypy
WorkerId = str
WorkerSpec = Tuple[
    WorkerId,
    Optional[str],  # name
    Creator[Station],
]
