from flexplan import Message, Workshop, Worker


class One(Worker):
    def greet(self):
        print("One!")
        Message(Two.greet).emit()


class Two(Worker):
    def greet(self):
        print("Two!")
        Message(Three.greet).emit()


class Three(Worker):
    def greet(self):
        print("Three!")


if __name__ == "__main__":
    import time

    workshop = Workshop()
    workshop.register(One)
    workshop.register(Two)
    workshop.register(Three)
    with workshop:
        workshop.submit(One.greet)
        time.sleep(1)
