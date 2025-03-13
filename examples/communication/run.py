from flexplan import Message, Workshop


class FirstWorker:
    def greet(self):
        print("One!")
        Message(SecondWorker.greet).emit()


class SecondWorker:
    def greet(self):
        print("Two!")
        Message(ThirdWorker.greet).emit()


class ThirdWorker:
    def greet(self):
        print("Three!")


if __name__ == "__main__":
    import time

    workshop = Workshop()
    workshop.register(FirstWorker)
    workshop.register(SecondWorker)
    workshop.register(ThirdWorker)
    with workshop:
        workshop.submit(FirstWorker.greet)
        time.sleep(1)
