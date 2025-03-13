import os
from flexplan import Workshop


class SimpleWorker:
    def greet(self):
        print(f"Hello world! The pid of worker is {os.getpid()}")
        return os.getpid()


if __name__ == "__main__":
    workshop = Workshop()
    workshop.register(SimpleWorker, station="process")
    with workshop:
        print(f"The pid of main program is {os.getpid()}")
        future = workshop.submit(SimpleWorker.greet)
        print(f"The result is {future.result()}")
