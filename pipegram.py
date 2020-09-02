from typing import Any, Callable, Dict, Hashable, Iterable, NoReturn, List, Set, Tuple, Optional, Union

import math
import os
import sys
import signal
import threading
import multiprocessing as mp
from multiprocessing.pool import ApplyResult

Number = Union[int, float]


class DependencyChain:
    def __init__(self):
        self.__priority_of = dict()
        self.__sub_of = dict()
        self.__sup_of = dict()
        self.__len = 0

    def add(self, name: Hashable, after: Iterable[Hashable] = None):
        if name is None:
            raise ValueError('param "name" should not be None')
        if name in self.__priority_of:
            raise ValueError(f'item {name} already exists')
        if after is None:
            self.__sup_of[name] = set()
        else:
            self.__sup_of[name] = {after} if isinstance(after, str) else after

        if name not in self.__sub_of:
            self.__sub_of[name] = set()
        for pw in self.__sup_of[name]:
            if pw not in self.__sub_of:
                self.__sub_of[pw] = set()
            self.__sub_of[pw].add(name)

        self.__priority_of[name] = 0
        self.__update_priority_of(name)

    def sub_of(self, item: Hashable) -> Set[Hashable]:
        return self.__sub_of[item]

    def sup_of(self, item: Hashable) -> Set[Hashable]:
        return self.__sup_of[item]

    def __update_priority_of(self, name):
        if 0 == len(self.__sub_of[name]) == len(self.__sup_of[name]):
            self.__priority_of[name] = -1
            return
        priority = self.__priority_of[name]
        for pw in self.__sup_of[name]:
            if pw not in self.__priority_of or -2 == self.__priority_of[pw]:
                self.__priority_of[name] = -2
                return
            elif self.__priority_of[pw] == -1:
                self.__priority_of[pw] = 0
                priority = max(priority, 1)
            else:
                priority = max(priority, self.__priority_of[pw] + 1)
        self.__priority_of[name] = priority
        self.__len = max(self.__len, priority + 1)
        for sw in self.__sub_of[name]:
            self.__update_priority_of(sw)

    def __getitem_core(self, index: int) -> Set[Hashable]:
        res = set()
        for key, value in self.__priority_of.items():
            if value == index:
                res.add(key)
        return res

    def __contains__(self, item) -> bool:
        return item in self.__priority_of

    def __getitem__(self, index: int) -> Set[Hashable]:
        if index < 0 or index >= self.__len:
            raise IndexError('index out of range')
        return self.__getitem_core(index)

    def __iter__(self):
        level = 0
        length = self.__len
        while level < length:
            yield self.__getitem__(level)
            level += 1

    def __len__(self):
        return self.__len

    def __str__(self):
        if self.__len > 0:
            res = list(map(lambda index, task: f'{index}: {task}; ', list(enumerate(self))))
        else:
            res = list()
        ind = self.independent_items()
        res.append('independent: {}; '.format(ind if len(ind) > 0 else '{}'))
        ivd = self.invalid_items()
        res.append('invalid: {}; '.format(ivd if len(ivd) > 0 else '{}'))
        return ''.join(res)

    def size(self) -> int:
        return len(self.__priority_of)

    def invalid_items(self) -> Set[Hashable]:
        return self.__getitem_core(-2)

    def independent_items(self) -> Set[Hashable]:
        return self.__getitem_core(-1)

    def dependent_items(self) -> Set[Hashable]:
        all_items = set(self.__priority_of.keys())
        return all_items - self.__getitem_core(-2) - self.__getitem_core(-1)


class Workflow:
    class Placeholder:
        def __init__(self, name: Hashable):
            self.__name = name

        @property
        def name(self):
            return self.__name

        def __str__(self):
            return f'Placeholder for "{self.__name}"'

    class Task:
        def __init__(self, name: Hashable, func: Callable, args: tuple = None, kwargs: dict = None,
                     error: str = 'raise', coerce=None):
            self.func = func
            self.args = args if args is not None else tuple()
            self.kwargs = kwargs if kwargs is not None else dict()
            self.__name: Hashable = name
            self.__res: Optional[ApplyResult] = None
            self.__ready: bool = False
            self.__got: bool = False
            self.__ret = None
            self.__err = error
            self.__cr = coerce

        def invoked(self) -> bool:
            return self.__res is not None

        @property
        def name(self):
            return self.__name

        @property
        def error_strategy(self):
            return self.__err

        @property
        def coerce(self):
            return self.__cr

        @property
        def apply_result(self) -> Optional[ApplyResult]:
            return self.__res

        @apply_result.setter
        def apply_result(self, value: ApplyResult):
            self.__res = value

        def ready(self) -> bool:
            if self.__ready:
                return True
            elif self.__res is None or not self.__res.ready():
                return False
            else:
                self.__ready = True
                return True

        def __handle_exc(self, exc: Exception):
            if (err := self.__err) == 'raise':
                self.__ret = exc
                raise RuntimeError(f'RuntimeError in {self.__name}') from exc
            elif err == 'ignore':
                pass
            elif err == 'coerce':
                if callable(cr := self.__cr):
                    self.__ret = cr(exc)
                else:
                    self.__ret = cr
            else:
                raise ValueError(f'unrecognized error handler "{err}"') from None

        def get(self, timeout: Number = 15) -> Any:
            if self.__got:
                return self.__ret
            try:
                self.__ret = self.__res.get(timeout)
            except Exception as exc:
                self.__handle_exc(exc)
            finally:
                self.__ready = True
                self.__got = True
            return self.__ret

        def get_local(self) -> Any:
            if self.__got:
                return self.__ret
            try:
                self.__ret = self.func(*self.args, **self.kwargs)
                self.__got = True
            except Exception as exc:
                self.__handle_exc(exc)
            finally:
                self.__ready = True
            return self.__ret

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
            self.__all_done = False
            self.__done_set = set()
            self.__tasks = tasks

        def ready(self, name: Hashable = None) -> bool:
            if name is not None and name not in self.__tasks:
                raise KeyError(f'Task {name} not found')
            if self.__all_done:
                return True
            if name is not None and name in self.__done_set:
                return True
            for task_name, task in self.__tasks.items():
                if task_name in self.__done_set:
                    continue
                if task.ready():
                    self.__done_set.add(task_name)
                elif name is None:
                    return False
            if len(self.__done_set) == len(self.__tasks):
                self.__all_done = True
                return True
            if name in self.__done_set:
                return True
            return False

        def get(self, name: Hashable = None) -> Union[Dict[Hashable, Any], Any]:
            if name is None:
                res = dict()
                for task_name, task in self.__tasks.items():
                    if self.ready(task_name):
                        res[task_name] = task.get()
                return res
            else:
                return self.__tasks[name].get() if self.ready(name) else None

    def __init__(self,
                 workers: int = None,
                 method: str = 'mix',
                 maxtasksperchild: int = 12,
                 independent_ratio: float = 0.25,
                 check_interval: float = 0.2,
                 single_thread_pool: bool = False):
        self.__n_workers = os.cpu_count() if workers is None else int(max(workers, 1))
        self.__method = method
        if self.__method not in ('mix', 'bfs', 'dfs'):
            raise ValueError(f'unrecognized method "{self.__method}"')
        self.__maxtasksperchild = maxtasksperchild
        self.__ir = max(min(independent_ratio, 1.0), 0.05)
        self.__n_ind_workers = math.ceil(self.__n_workers * self.__ir)
        self.__n_dep_workers = self.__n_workers - self.__n_ind_workers
        self.__interval = max(check_interval, 0.001)
        self.__chain = DependencyChain()
        self.__tasks: Dict[Hashable, Workflow.Task] = dict()
        self.__1thd_pool = single_thread_pool
        self.__heartbeat_handler: Optional[List[Callable, Tuple, Dict]] = None
        self.__detached_thread: Optional[threading.Thread] = None
        # TODO: short circuit and broken circuit
        self.__short_c = list()
        self.__broken_c = list()

    def add(self, name: Hashable, task: Callable,
            args: tuple = None, kwargs: dict = None, after: Iterable[Hashable] = None,
            error: str = 'raise', coerce=None):
        if name is None:
            raise ValueError('param "name" should not be None')
        self.__chain.add(name, after=after)
        self.__tasks[name] = Workflow.Task(name, task, args=args, kwargs=kwargs,
                                           error=error, coerce=coerce)

    def set_heartbeat_handler(self, handler: Union[Callable, 'Workflow'], args: tuple = None, kwargs: dict = None):
        if self.__heartbeat_handler is not None:
            raise ValueError('handler was set twice')
        self.__heartbeat_handler = (handler,
                                    args if args is not None else tuple(),
                                    kwargs if kwargs is not None else dict())

    @classmethod
    def p(cls, name):
        return cls.Placeholder(name)

    def __getitem__(self, name: Hashable) -> 'Workflow.Task':
        return self.__tasks[name]

    def size(self) -> int:
        return self.__chain.size()

    def invalid_items(self) -> Set[Hashable]:
        return self.__chain.invalid_items()

    def independent_items(self) -> Set[Hashable]:
        return self.__chain.independent_items()

    def dependent_items(self) -> Set[Hashable]:
        return self.__chain.dependent_items()

    @classmethod
    def __heartbeat(cls, event: threading.Event, interval: float,
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
    def __run_simple(cls, pool: mp.Pool,
                     tasks: Dict[Hashable, 'Workflow.Task'], interval: float,
                     handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = len(tasks)
        done_set = set()
        event = threading.Event()
        heartbeat = cls.__heartbeat

        for name, task in tasks.items():
            task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        while len(done_set) != n_tasks:
            heartbeat(event, interval, tasks, done_set=done_set, handler=handler)

    @classmethod
    def __apply_ind(cls, pool: mp.Pool, tasks: Dict[Hashable, 'Workflow.Task'],
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
    def __apply_bfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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
    def __apply_dfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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
    def __apply_dfs_recv(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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
                        cls.__apply_dfs_recv(pool, chain, tasks, done_set, visit_set, ss_sup)
            if all([s_sup in done_set for s_sup in s_sups]):
                task.fill_placeholders(tasks)
                task.apply_result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)

    @classmethod
    def __run_dfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                  n_dep_workers: int, n_ind_workers: int, interval: float,
                  handler: Tuple[Callable, Tuple, Dict] = None):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        dep_set = chain.dependent_items()
        ind_running_set = set()
        analyze = set()
        visit_set = set()

        apply_ind = cls.__apply_ind
        apply_dfs = cls.__apply_dfs_recv
        event = threading.Event()
        heartbeat = cls.__heartbeat

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
    def __run_bfs(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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

        apply_ind = cls.__apply_ind
        apply_bfs = cls.__apply_bfs
        event = threading.Event()
        heartbeat = cls.__heartbeat

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, dep_set, done_set, ind_running_set,
                      n_dep_workers, n_ind_workers)  # independent tasks
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            heartbeat(event, interval, tasks, done_set=done_set, handler=handler)

    @classmethod
    def __run_mix(cls, pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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

        apply_ind = cls.__apply_ind
        apply_dfs = cls.__apply_dfs
        apply_bfs = cls.__apply_bfs
        event = threading.Event()
        heartbeat = cls.__heartbeat

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, dep_set, done_set, ind_running_set,
                      n_dep_workers, n_ind_workers)  # independent tasks
            apply_dfs(pool, chain, tasks, done_set, analyze)  # dfs
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            heartbeat(event, interval, tasks, done_set=done_set, analyze=analyze, handler=handler)

    @classmethod
    def __init_pool(cls, processes: int, maxtasksperchild: int) -> mp.Pool:
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

    def __run_parallel(self):
        n_level = len(self.__chain)
        with self.__init_pool(self.__n_workers, self.__maxtasksperchild) as pool:
            if n_level == 0:  # all independent tasks
                self.__run_simple(pool, self.__tasks, interval=self.__interval, handler=self.__heartbeat_handler)
            else:
                if (m := self.__method) == 'mix':
                    run_core = self.__run_mix
                elif m == 'bfs':
                    run_core = self.__run_bfs
                elif m == 'dfs':
                    run_core = self.__run_dfs
                else:
                    raise ValueError(f'unrecognized method "{m}"')
                run_core(pool, self.__chain, self.__tasks,
                         n_dep_workers=self.__n_dep_workers, n_ind_workers=self.__n_ind_workers,
                         interval=self.__interval, handler=self.__heartbeat_handler)

    def __run_single(self):
        chain = self.__chain
        tasks = self.__tasks
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

    def __reset_tasks(self):
        tasks = dict()
        for name, task in self.__tasks.items():
            tasks[name] = Workflow.Task(task.name, task.func,
                                        args=task.args, kwargs=task.kwargs,
                                        error=task.error_strategy, coerce=task.coerce)
        self.__tasks = tasks

    def run(self) -> Optional['Workflow.Result']:
        invalid_set = self.__chain.invalid_items()
        if len(invalid_set) > 0:
            raise ValueError(f'invalid task(s) exists: {invalid_set}')
        if self.__chain.size() == 0:
            return None

        result = Workflow.Result(self.__tasks)
        if result.ready():
            self.__reset_tasks()
            result = Workflow.Result(self.__tasks)
        if self.__n_workers == 1 and not self.__1thd_pool and self.__heartbeat_handler is None:
            self.__run_single()
        else:
            self.__run_parallel()
        return result

    def run_detached(self) -> Optional['Workflow.Result']:
        invalid_set = self.__chain.invalid_items()
        if len(invalid_set) > 0:
            raise ValueError(f'invalid task(s) exists: {invalid_set}')

        result = Workflow.Result(self.__tasks)
        if self.__detached_thread is not None:
            if not result.ready():
                raise RuntimeError('failed to invoke run_detached() as '
                                   'workflow has been triggered and is still running')
            self.__reset_tasks()
            result = Workflow.Result(self.__tasks)
        elif result.ready():
            self.__reset_tasks()
            result = Workflow.Result(self.__tasks)

        self.__detached_thread = threading.Thread(target=self.run)
        self.__detached_thread.start()
        return result

    def join(self, timeout: Number = None) -> NoReturn:
        if self.__detached_thread is None:
            raise RuntimeError('failed to invoke join() method as workflow is not running in detached mode')
        self.__detached_thread.join(timeout)
        self.__detached_thread = None
