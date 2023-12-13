from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Any, Protocol

    class PickleLike(Protocol):
        def loads(self, data: bytes) -> Any:
            ...

        def dumps(self, obj: Any) -> bytes:
            ...

def get_pickle(*preferences: str) -> "PickleLike":
    if len(preferences) == 0:
        preferences = ("cloudpickle", "dill", "pickle")
    if len(preferences) == 1:
        return __import__(preferences[0])
    for p in preferences:
        try:
            return __import__(p)
        except ImportError:
            pass
    raise ImportError(f"Could not find any of {preferences}")