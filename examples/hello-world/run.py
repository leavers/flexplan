from flexplan import Workshop


class SimpleWorker:
    def greet(self):
        print("Hello world!")


if __name__ == "__main__":
    workshop = Workshop()
    workshop.register(SimpleWorker)
    with workshop:
        workshop.submit(SimpleWorker.greet)
