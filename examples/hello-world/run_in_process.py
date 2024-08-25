import os
from flexplan import Worker, Workshop


class SimpleWorker(Worker):
    def echo(self):
        print(f"Hello world! The pid of worker is {os.getpid()}")


if __name__ == "__main__":
    workshop = Workshop()
    workshop.register(SimpleWorker, station="process")
    with workshop:
        print(f"The pid of main program is {os.getpid()}")
        workshop.submit(SimpleWorker.echo)
