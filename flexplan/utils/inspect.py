from functools import cached_property, partial
from inspect import getmodule, getmro, isbuiltin, isfunction, ismethod

from typing_extensions import Any, Callable, Optional, ParamSpec, Self, Type, TypeVar

T = TypeVar("T")


def ispartial(obj: Any) -> bool:
    return isinstance(obj, partial)


def ispartialmethod(obj: Any) -> bool:
    return hasattr(obj, "_partialmethod") and callable(
        getattr(obj._partialmethod, "func", None)
    )

_warn_nested_class = False


def getmethodclass(method: Callable) -> Optional[Type]:
    """Get the class of a method, if not found then ``None`` is returned.

    Note that ``getmethodclass`` does not work with:

    *. Methods of nested classes, or
    *. Patched functions such as ``MyTest.func = some_func``.

    If users call ``getmethodclass`` with these fucntion, then ``None`` is returned.

    :type method: callable
    :param method: The function or method to be inspected.

    :rtype: class type of ``None``
    :return: Bounding class of ``method`` if exists.
    """

    if isinstance(method, partial):
        return getmethodclass(method.func)
    if ispartialmethod(method):
        return getmethodclass(getattr(getattr(method, "_partialmethod"), "func"))
    if hasattr(method, "__wrapped__"):
        return getmethodclass(getattr(method, "__wrapped__"))
    if isinstance(method, property):
        if method.fget:
            return getmethodclass(method.fget)
        elif method.fset:
            return getmethodclass(method.fset)
        return None
    if isinstance(method, cached_property):
        return getmethodclass(method.func)
    if ismethod(method) or (
        isbuiltin(method)
        and hasattr(method, "__self__")
        and hasattr(method.__self__, "__class__")
    ):
        for cls in getmro(method.__self__.__class__):
            if method.__name__ in cls.__dict__:
                return cls
        method = getattr(method, "__func__", method)
    if isfunction(method):
        cls = getattr(  # type: ignore[assignment]
            getmodule(method),
            method.__qualname__.split(".<locals>", 0)[0].rsplit(".", 1)[0],
            None,
        )
        if isinstance(cls, type):
            return cls
        elif ".<locals>" in method.__qualname__:
            import warnings

            global _warn_nested_class

            if not _warn_nested_class:
                _warn_nested_class = True
                with warnings.catch_warnings():
                    warnings.simplefilter("once", RuntimeWarning)
                    warnings.warn(
                        "Function `getmethodclass` does not methods of nested classes, "
                        f"method={method}",
                        RuntimeWarning,
                    )
    return getattr(method, "__objclass__", None)
