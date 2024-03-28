from typing_extensions import Optional, Tuple

from flexplan.datastructures.instancecreator import InstanceCreator
from flexplan.stations.base import Station

StationSpec = Tuple[Optional[str], InstanceCreator[Station]]
