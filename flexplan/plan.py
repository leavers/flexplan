import typing_extensions as t

t.Annotated

class Plan:
    def __init__(self):
        pass

    def start_with(self) -> t.Self:
        return self

    def then(self) -> t.Self:
        return self

    def goto(self) -> t.Self:
        return self