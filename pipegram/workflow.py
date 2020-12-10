import math
import multiprocessing as mp
import os
import signal
import sys
import threading
from typing import (
    Any, Callable, Coroutine, Dict, Hashable, Iterable,
    List, Set, Tuple, Optional, Union
)

from pipegram.common import (
    Dependent,
    WorkflowMode, workflow_modes,
    WORKFLOW_MODE_MIX, WORKFLOW_MODE_BFS, WORKFLOW_MODE_DFS,
    ErrorMode, error_modes,
    ERROR_MODE_RAISE, ERROR_MODE_IGNORE, ERROR_MODE_COERCE
)
from pipegram.dependency_chain import DependencyChain
from pipegram.executor import HybridPoolExecutor, PollFuture


class Placeholder:
    def __init__(
            self,
            name: Hashable,
            handler: Callable[..., Any] = None,
            handler_args: Tuple[Any, ...] = (),
            handler_kwargs: Dict[str, Any] = None
    ):
        if name is None:
            raise TypeError('Param "name" should not be None.')
        if handler and not callable(handler):
            raise TypeError('Param "handler" is not callable.')
        self._name = name
        self._handler = handler
        self._h_args = handler_args or ()
        self._h_kwargs = handler_kwargs or {}

    @property
    def name(self):
        return self._name

    def handle(self, value):
        return value if not self._handler else \
            self._handler(value, *self._h_args, **self._h_kwargs)

    def __str__(self):
        return f'Placeholder for "{self._name}"'


class Task:
    def __init__(
            self,
            name: Hashable,
            func: Callable[..., Any],
            args: Tuple[Any, ...] = (),
            kwargs: Dict[str, Any] = None,
            after: Dependent = None,
            on_error: ErrorMode = ERROR_MODE_RAISE,
            coerce: Union[Callable[..., Any], Any] = None,
            coerce_args: Tuple[Any, ...] = (),
            coerce_kwargs: Dict[str, Any] = None
    ):
        if on_error not in error_modes:
            raise ValueError(f'Unrecognized error handler "{on_error}".')
        self._name = name
        self._func = func
        self._args = args
        self._kwargs = kwargs or {}
        self._future: Optional[PollFuture] = None
        self._after = after
        self._err_mode = on_error
        self._cr = coerce
        self._cr_args = coerce_args
        self._cr_kwargs = coerce_kwargs or {}

    def invoked(self) -> bool:
        return self._future is not None

    @property
    def name(self) -> Hashable:
        return self._name

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    @property
    def after(self) -> Dependent:
        return self._after

    @property
    def error_mode(self):
        return self._err_mode

    def ready(self) -> bool:
        return self._future.ready()

    def get(self) -> Any:
        result = None
        try:
            result = self._future.get()
        except Exception as exc:
            err_mode = self._err_mode
            if err_mode == ERROR_MODE_RAISE:
                raise RuntimeError(f'RuntimeError in {self._name}') \
                    from self._future.exception
            elif err_mode == ERROR_MODE_COERCE:
                if callable(self._cr):
                    return self._cr(exc, result,
                                    *self._cr_args, **self._cr_kwargs)
                return self._cr
        return result

    @classmethod
    def _fill_ph(cls, items: Dict[Hashable, 'Task'], val):
        if not isinstance(val, (Placeholder, tuple, list, set, dict)):
            return val
        elif isinstance(val, Placeholder):
            return val.handle(items[val.name].get())
        elif isinstance(val, (tuple, list, set)):
            clz = type(val)
            arg_list = list(val)
            for i, arg in enumerate(arg_list):
                arg_list[i] = cls._fill_ph(items, arg)
            return clz(arg_list)
        elif isinstance(val, dict):
            for k, v in val.items():
                val[k] = cls._fill_ph(items, v)
            return dict(val)

    def fill_placeholders(self, items: Dict[Hashable, 'Task']):
        self._args = self._fill_ph(items, self._args)
        self._kwargs = self._fill_ph(items, self._kwargs)


class Workflow:
    class Result:
        def __init__(self, tasks: Dict[Hashable, 'Task']):
            self._all_done = False
            self._done_set = set()
            self._tasks = tasks

        def ready(self, name: Hashable = None) -> bool:
            if name is not None and name not in self._tasks:
                raise KeyError(f'Task {name} not found')
            if self._all_done:
                return True
            if name is not None and name in self._done_set:
                return True
            for task_name, task in self._tasks.items():
                if task_name in self._done_set:
                    continue
                if task.ready():
                    self._done_set.add(task_name)
                elif name is None:
                    return False
            if len(self._done_set) == len(self._tasks):
                self._all_done = True
                return True
            if name in self._done_set:
                return True
            return False

        def get(self, name: Hashable = None) -> Union[Dict[Hashable, Any], Any]:
            if name is None:
                res = dict()
                for task_name, task in self._tasks.items():
                    if self.ready(task_name):
                        res[task_name] = task.get()
                return res
            else:
                return self._tasks[name].get() if self.ready(name) else None

    def __init__(
            self,
            mode: WorkflowMode = WORKFLOW_MODE_MIX,
            executor: HybridPoolExecutor = None,
            chain: DependencyChain = None,
            independent_ratio: float = 0.25,
            wait_interval: float = 0.1
    ):
        if mode not in workflow_modes:
            raise ValueError(f'Unrecognized mode "{mode}".')
        self._mode = mode
        if executor is None:
            self._executor = HybridPoolExecutor()
        else:
            if not isinstance(executor, HybridPoolExecutor):
                raise TypeError('Param "executor" is expected to be a HybridPoolExecutor, '
                                f'got {type(executor)} instead.')
            self._executor = executor
        if chain is None:
            self._chain = DependencyChain()
        else:
            if not isinstance(chain, DependencyChain):
                raise TypeError('Param "chain" is expected to be a DependencyChain, '
                                f'got {type(chain)} instead.')
            self._chain = chain
        self._ir = max(min(independent_ratio, 1.0), 0.05)
        self._interval = max(wait_interval, 1e-6)
        self._tasks: Dict[Hashable, Task] = {}
        self._detached_thd: Optional[threading.Thread] = None

    def add(self, task: Task):
        if not isinstance(task, Task):
            raise TypeError('Param "task" is expected to be a Task, '
                            f'got {type(task)} instead.')
        name = task.name
        if name in self._tasks:
            raise KeyError(f'Task "{name}" already exists.')
        self._chain.add(name, after=task.after)
        self._tasks[name] = task

    def remove(self, task: Union[Task, Hashable]):
        name = task.name if isinstance(task, Task) else task
        self._chain.remove(name)
        self._tasks.pop(name)

    def ignore(self, task: Union[Task, Hashable]):
        name = task.name if isinstance(task, Task) else task
        self._chain.ignore(name)
        self._tasks.pop(name)

