import math
import multiprocessing as mp
import os
import signal
import sys
import threading
from multiprocessing.pool import ApplyResult
from typing import Any, Callable, Dict, Hashable, Iterable, NoReturn, List, Set, Tuple, Optional, Union

from dependency_chain import DependencyChain
from types_ import Number


class Workflow:
    class Placeholder:
        def __init__(self, name: Hashable):
            self._name = name

        @property
        def name(self):
            return self._name

        def __str__(self):
            return f'Placeholder for "{self._name}"'

    class Task:
        def __init__(self, name: Hashable, func: Callable, args: tuple = None, kwargs: dict = None,
                     error: str = 'raise', coerce=None):
            self.func = func
            self.args = args if args is not None else tuple()
            self.kwargs = kwargs if kwargs is not None else dict()
            self._name: Hashable = name
            self._res: Optional[ApplyResult] = None
            self._ready: bool = False
            self._got: bool = False
            self._ret = None
            self._err = error
            self._cr = coerce

        def invoked(self) -> bool:
            return self._res is not None

        @property
        def name(self):
            return self._name

        @property
        def error_strategy(self):
            return self._err

        @property
        def coerce(self):
            return self._cr

        @property
        def apply_result(self) -> Optional[ApplyResult]:
            return self._res

        @apply_result.setter
        def apply_result(self, value: ApplyResult):
            self._res = value

        def ready(self) -> bool:
            if self._ready:
                return True
            elif self._res is None or not self._res.ready():
                return False
            else:
                self._ready = True
                return True

        def _handle_exc(self, exc: Exception):
            if (err := self._err) == 'raise':
                self._ret = exc
                raise RuntimeError(f'RuntimeError in {self._name}') from exc
            elif err == 'ignore':
                pass
            elif err == 'coerce':
                if callable(cr := self._cr):
                    self._ret = cr(exc)
                else:
                    self._ret = cr
            else:
                raise ValueError(f'unrecognized error handler "{err}"') from None

        def get(self, timeout: Number = 15) -> Any:
            if self._got:
                return self._ret
            try:
                self._ret = self._res.get(timeout)
            except Exception as exc:
                self._handle_exc(exc)
            finally:
                self._ready = True
                self._got = True
            return self._ret

        def get_local(self) -> Any:
            if self._got:
                return self._ret
            try:
                self._ret = self.func(*self.args, **self.kwargs)
                self._got = True
            except Exception as exc:
                self._handle_exc(exc)
            finally:
                self._ready = True
            return self._ret

        @classmethod
        def _fill_ph(cls, items: Dict[Hashable, 'Workflow.Task'], key: Hashable,
                     handler: Callable = None, h_args: tuple = None, h_kwargs: dict = None):
            ret = items[key].get()
            return ret if handler is None else handler(ret, *h_args, **h_kwargs)

        @classmethod
        def _fill_ph_rec(cls, items: Dict[Hashable, 'Workflow.Task'], val,
                         handler: Callable = None, h_args: tuple = None, h_kwargs: dict = None):
            if not isinstance(val, (Workflow.Placeholder, tuple, list, set, dict)):
                return val
            elif isinstance(val, Workflow.Placeholder):
                return Workflow.Task._fill_ph(items, val.name, handler, h_args, h_kwargs)
            elif isinstance(val, (tuple, list, set)):
                clz = type(val)
                arg_list = list(val)
                for i, arg in enumerate(arg_list):
                    arg_list[i] = cls._fill_ph_rec(items, arg, handler, h_args, h_kwargs)
                return clz(arg_list)
            elif isinstance(val, dict):
                for k, v in val.items():
                    val[k] = cls._fill_ph_rec(items, v, handler, h_args, h_kwargs)
                return dict(val)

        def fill_placeholders(self, items: Dict[Hashable, 'Workflow.Task'],
                              handler: Callable = None, h_args: tuple = None, h_kwargs: dict = None):
            if h_args is None:
                h_args = tuple()
            if h_kwargs is None:
                h_kwargs = dict()
            self.args = self._fill_ph_rec(items, self.args, handler, h_args, h_kwargs)
            self.kwargs = self._fill_ph_rec(items, self.kwargs, handler, h_args, h_kwargs)

    class Result:
        def __init__(self, tasks: Dict[Hashable, 'Workflow.Task']):
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

    def __init__(self,
                 workers: int = None,
                 method: str = 'mix',
                 maxtasksperchild: int = 12,
                 independent_ratio: float = 0.25,
                 check_interval: float = 0.2,
                 single_thread_pool: bool = False):
        self._n_workers = os.cpu_count() if workers is None else int(max(workers, 1))
        self._method = method
        if self._method not in ('mix', 'bfs', 'dfs'):
            raise ValueError(f'unrecognized method "{self._method}"')
        self._maxtasksperchild = maxtasksperchild
        self._ir = max(min(independent_ratio, 1.0), 0.05)
        self._n_ind_workers = math.ceil(self._n_workers * self._ir)
        self._n_dep_workers = self._n_workers - self._n_ind_workers
        self._interval = max(check_interval, 0.001)
        self._chain = DependencyChain()
        self._tasks: Dict[Hashable, Workflow.Task] = dict()
        self._1thd_pool = single_thread_pool
        self._heartbeat_handler: Optional[List[Callable, Tuple, Dict]] = None
        self._detached_thread: Optional[threading.Thread] = None
        # TODO: short circuit and broken circuit
        self._short_c = list()
        self._broken_c = list()

    def add(self, name: Hashable, task: Callable,
            args: tuple = None, kwargs: dict = None, after: Iterable[Hashable] = None,
            error: str = 'raise', coerce=None):
        if name is None:
            raise ValueError('param "name" should not be None')
        self._chain.add(name, after=after)
        self._tasks[name] = Workflow.Task(name, task, args=args, kwargs=kwargs,
                                          error=error, coerce=coerce)

    def set_heartbeat_handler(self, handler: Union[Callable, 'Workflow'], args: tuple = None, kwargs: dict = None):
        if self._heartbeat_handler is not None:
            raise ValueError('handler was set twice')
        self._heartbeat_handler = (handler,
                                   args if args is not None else tuple(),
                                   kwargs if kwargs is not None else dict())

    @classmethod
    def p(cls, name):
        return cls.Placeholder(name)

    def __getitem__(self, name: Hashable) -> 'Workflow.Task':
        return self._tasks[name]

    def size(self) -> int:
        return self._chain.size()

    def invalid_items(self) -> Set[Hashable]:
        return self._chain.invalid_items()

    def independent_items(self) -> Set[Hashable]:
        return self._chain.independent_items()

    def dependent_items(self) -> Set[Hashable]:
        return self._chain.dependent_items()

    @classmethod
    def _heartbeat(cls, event: threading.Event, interval: float,
                   tasks: Dict[Hashable, 'Workflow.Task'], done_set: Set[Hashable], analyze: Set[Hashable] = None,
                   handler: Tuple[Callable, Tuple, Dict] = None):
        for name, task in tasks.items():  # check status
            if not task.ready() or name in done_set:
                continue
            task.get()
            done_set.add(name)
            if analyze is not None:
                analyze.add(name)
        if handler is not None:
            func = handler[0]
            if isinstance(func, Workflow):
                func.run()
            else:
                func(*handler[1], **handler[2])
        event.wait(interval)

    @classmethod
    def _run_simple(cls, pool: mp.Pool,
                    tasks: Dict[Hashable, 'Workflow.Task'], interval: float,
                    handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = len(tasks)
        done_set = set()
        event = threading.Event()
        heartbeat = cls._heartbeat

        for name, task in tasks.items():
            task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        while len(done_set) != n_tasks:
            heartbeat(event, interval, tasks, done_set=done_set, handler=handler)

    @classmethod
    def _apply_ind(cls, pool: mp.Pool, tasks: Dict[Hashable, 'Workflow.Task'],
                   ind_set: set, dep_set: set, done_set: set, ind_running_set: set,
                   n_dep_workers: int, n_ind_workers: int):
        if all([t in done_set for t in ind_set]):
            return
        ind_running_set -= done_set
        run_all = len(dep_set - done_set) <= n_dep_workers
        if len(ind_running_set) < n_ind_workers:
            for t in ind_set:
                task = tasks[t]
                if task.invoked():
                    continue
                task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
                ind_running_set.add(t)
                if run_all:
                    continue
                if len(ind_running_set) >= n_ind_workers:
                    break

    @classmethod
    def _apply_bfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                   level_tasks: set, done_set: set, curr_level: int, n_levels: int):
        if all([t in done_set for t in level_tasks]):  # bfs
            if curr_level + 1 < n_levels:
                curr_level += 1
                level_tasks = chain[curr_level]
        else:
            for t in level_tasks:
                task = tasks[t]
                if not task.invoked():
                    task.fill_placeholders(tasks)
                    task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        return curr_level, level_tasks

    @classmethod
    def _apply_dfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                   done_set: set, analyze: set):
        for item in analyze:  # dfs
            subs = chain.sub_of(item)
            for sub in subs:
                task = tasks[sub]
                s_sups = chain.sup_of(sub)
                if not task.invoked() and all([s_sup in done_set for s_sup in s_sups]):
                    task.fill_placeholders(tasks)
                    task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        analyze.clear()

    @classmethod
    def _apply_dfs_recv(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                        done_set: set, visit_set: set, name: Hashable):
        subs = chain.sub_of(name)
        for sub in subs:
            task = tasks[sub]
            s_sups = chain.sup_of(sub)
            if task.invoked():
                continue
            for s_sup in s_sups:
                if s_sup == name or s_sup in visit_set:
                    continue
                s_task = tasks[s_sup]
                if s_task.invoked():
                    continue
                else:
                    visit_set.add(s_sup)
                    ss_sups = chain.sup_of(s_sup)
                    for ss_sup in ss_sups:
                        cls._apply_dfs_recv(pool, chain, tasks, done_set, visit_set, ss_sup)
            if all([s_sup in done_set for s_sup in s_sups]):
                task.fill_placeholders(tasks)
                task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)

    @classmethod
    def _run_dfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                 n_dep_workers: int, n_ind_workers: int, interval: float,
                 handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        dep_set = chain.dependent_items()
        ind_running_set = set()
        analyze = set()
        visit_set = set()

        apply_ind = cls._apply_ind
        apply_dfs = cls._apply_dfs_recv
        event = threading.Event()
        heartbeat = cls._heartbeat

        for t in chain[0]:
            task = tasks[t]
            task.fill_placeholders(tasks)
            task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, dep_set, done_set, ind_running_set,
                      n_dep_workers, n_ind_workers)  # independent tasks
            for item in analyze:  # dfs
                apply_dfs(pool, chain, tasks, done_set, visit_set, item)
            visit_set.clear()
            analyze.clear()
            heartbeat(event, interval, tasks, done_set=done_set, analyze=analyze, handler=handler)

    @classmethod
    def _run_bfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                 n_dep_workers: int, n_ind_workers: int, interval: float,
                 handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        dep_set = chain.dependent_items()
        curr_level = 0
        n_levels = len(chain)
        level_tasks = chain[curr_level]
        ind_running_set = set()

        apply_ind = cls._apply_ind
        apply_bfs = cls._apply_bfs
        event = threading.Event()
        heartbeat = cls._heartbeat

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, dep_set, done_set, ind_running_set,
                      n_dep_workers, n_ind_workers)  # independent tasks
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            heartbeat(event, interval, tasks, done_set=done_set, handler=handler)

    @classmethod
    def _run_mix(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                 n_dep_workers: int, n_ind_workers: int, interval: float,
                 handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        dep_set = chain.dependent_items()
        curr_level = 0
        n_levels = len(chain)
        level_tasks = chain[curr_level]
        ind_running_set = set()
        analyze = set()

        apply_ind = cls._apply_ind
        apply_dfs = cls._apply_dfs
        apply_bfs = cls._apply_bfs
        event = threading.Event()
        heartbeat = cls._heartbeat

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, dep_set, done_set, ind_running_set,
                      n_dep_workers, n_ind_workers)  # independent tasks
            apply_dfs(pool, chain, tasks, done_set, analyze)  # dfs
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            heartbeat(event, interval, tasks, done_set=done_set, analyze=analyze, handler=handler)

    @classmethod
    def _init_pool(cls, processes: int, maxtasksperchild: int) -> mp.Pool:
        if sys.platform == 'linux':
            signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT, signal.SIGTERM])
            pool = mp.Pool(processes=processes, maxtasksperchild=maxtasksperchild)
            signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGINT, signal.SIGTERM])
        else:
            orig_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
            orig_sigterm = signal.signal(signal.SIGTERM, signal.SIG_IGN)
            pool = mp.Pool(processes=processes, maxtasksperchild=maxtasksperchild)
            signal.signal(signal.SIGINT, orig_sigint)
            signal.signal(signal.SIGTERM, orig_sigterm)
        return pool

    def _run_parallel(self):
        n_level = len(self._chain)
        with self._init_pool(self._n_workers, self._maxtasksperchild) as pool:
            if n_level == 0:  # all independent tasks
                self._run_simple(pool, self._tasks, interval=self._interval, handler=self._heartbeat_handler)
            else:
                if (m := self._method) == 'mix':
                    run_core = self._run_mix
                elif m == 'bfs':
                    run_core = self._run_bfs
                elif m == 'dfs':
                    run_core = self._run_dfs
                else:
                    raise ValueError(f'unrecognized method "{m}"')
                run_core(pool, self._chain, self._tasks,
                         n_dep_workers=self._n_dep_workers, n_ind_workers=self._n_ind_workers,
                         interval=self._interval, handler=self._heartbeat_handler)

    def _run_single(self):
        chain = self._chain
        tasks = self._tasks
        ind_set = chain.independent_items()
        for name in ind_set:
            task = tasks[name]
            task.get_local()
        for level_tasks in chain:
            for t in level_tasks:
                task = tasks[t]
                if not task.invoked():
                    task.fill_placeholders(tasks)
                    task.get_local()

    def _reset_tasks(self):
        tasks = dict()
        for name, task in self._tasks.items():
            tasks[name] = Workflow.Task(task.name, task.func,
                                        args=task.args, kwargs=task.kwargs,
                                        error=task.error_strategy, coerce=task.coerce)
        self._tasks = tasks

    def run(self) -> Optional['Workflow.Result']:
        invalid_set = self._chain.invalid_items()
        if len(invalid_set) > 0:
            raise ValueError(f'invalid task(s) exists: {invalid_set}')
        if self._chain.size() == 0:
            return None

        result = Workflow.Result(self._tasks)
        if result.ready():
            self._reset_tasks()
            result = Workflow.Result(self._tasks)
        if self._n_workers == 1 and not self._1thd_pool and self._heartbeat_handler is None:
            self._run_single()
        else:
            self._run_parallel()
        return result

    def run_detached(self) -> Optional['Workflow.Result']:
        invalid_set = self._chain.invalid_items()
        if len(invalid_set) > 0:
            raise ValueError(f'invalid task(s) exists: {invalid_set}')

        result = Workflow.Result(self._tasks)
        if self._detached_thread is not None:
            if not result.ready():
                raise RuntimeError('failed to invoke run_detached() as '
                                   'workflow has been triggered and still running')
            self._reset_tasks()
            result = Workflow.Result(self._tasks)
        elif result.ready():
            self._reset_tasks()
            result = Workflow.Result(self._tasks)

        self._detached_thread = threading.Thread(target=self.run)
        self._detached_thread.start()
        return result

    def join(self, timeout: Number = None) -> NoReturn:
        if self._detached_thread is None:
            raise RuntimeError('failed to invoke join() method as workflow is not running in detached mode')
        self._detached_thread.join(timeout)
        self._detached_thread = None
