from flexplan.datastructures.initializer import Initializer
from flexplan.stations.base import Station


class ThreadStation(Station):
    """A station that runs a thread."""

    def __init__(self, name, thread):
        """Initialize a ThreadStation.

        Args:
            name (str): Name of the station.
            thread (Thread): Thread to run.
        """
        super().__init__(name)
        self.thread = thread

    def run(self):
        """Run the thread."""
        self.thread.start()
        self.thread.join()