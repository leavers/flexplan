from typing_extensions import Self

class Plan:
    def __init__(self):
        pass

    def start_with(self) -> Self:
        return self

    def then(self) -> Self:
        return self

    def goto(self) -> Self:
        return self