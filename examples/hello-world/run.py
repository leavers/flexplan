from flexplan import Worker, Workshop


class SimpleWorker(Worker):
    def greet(self):
        print("Hello world!")


if __name__ == "__main__":
    workshop = Workshop()
    workshop.register(SimpleWorker)
    with workshop:
        workshop.submit(SimpleWorker.greet)
