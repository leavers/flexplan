from typing import Callable, Dict, Iterable, Set, Hashable, Optional

import math
import os
import sys
import signal
import time
import multiprocessing as mp


class DependencyChain:
    def __init__(self):
        self.__priority_of = dict()
        self.__sub_item_of = dict()
        self.__sup_item_of = dict()
        self.__len = 0

    def add(self, name: Hashable, after: Iterable[Hashable] = None):
        if name in self.__priority_of:
            raise ValueError(f'{name} already exists')
        if after is None:
            self.__sup_item_of[name] = set()
        else:
            self.__sup_item_of[name] = {after} if isinstance(after, str) else after

        if name not in self.__sub_item_of:
            self.__sub_item_of[name] = set()
        for pw in self.__sup_item_of[name]:
            if pw not in self.__sub_item_of:
                self.__sub_item_of[pw] = set()
            self.__sub_item_of[pw].add(name)

        self.__priority_of[name] = 0
        self.__update_priority_of(name)

    def sub_of(self, item: Hashable) -> Set[Hashable]:
        return self.__sub_item_of[item]

    def sup_of(self, item: Hashable) -> Set[Hashable]:
        return self.__sup_item_of[item]

    def __update_priority_of(self, name):
        if 0 == len(self.__sub_item_of[name]) == len(self.__sup_item_of[name]):
            self.__priority_of[name] = -1
            return
        priority = self.__priority_of[name]
        for pw in self.__sup_item_of[name]:
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
        for sw in self.__sub_item_of[name]:
            self.__update_priority_of(sw)

    def __getitem_core(self, index: int) -> Set[Hashable]:
        res = set()
        for key, value in self.__priority_of.items():
            if value == index:
                res.add(key)
        return res

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


class Workflow:
    class Placeholder:
        def __init__(self, name):
            if name is None or isinstance(name, list):
                raise ValueError(f'name cannot be None or unhashable type, got {type(name)}')
            self.__name = name

        @property
        def name(self):
            return self.__name

    class Task:
        def __init__(self, func: Callable, args: tuple = None, kwargs: dict = None,
                     error: str = 'raise', coerce=None):
            self.func = func
            self.args = args if args is not None else tuple()
            self.kwargs = kwargs if kwargs is not None else dict()
            self.__res: Optional[mp.pool.ApplyResult] = None
            self.__ready: bool = False
            self.__ret = None
            self.__err = error
            self.__cr = coerce

        def invoked(self) -> bool:
            return self.__res is not None

        @property
        def result(self):
            return self.__res

        @result.setter
        def result(self, value):
            self.__res = value

        def ready(self) -> bool:
            if self.__ready:
                return True
            elif self.__res is None or not self.__res.ready():
                return False
            else:
                return True

        def __handle_exc(self, exc: Exception):
            if (err := self.__err) == 'raise':
                self.__ret = exc
                raise exc from exc
            elif err == 'ignore':
                pass
            elif err == 'coerce':
                if callable(cr := self.__cr):
                    self.__ret = cr(exc)
                else:
                    self.__ret = cr
            else:
                raise ValueError(f'unrecognized error handler "{err}"') from None

        def get(self, timeout: int = 15):
            if not self.__ready:
                try:
                    self.__ret = self.__res.get(timeout)
                except Exception as exc:
                    self.__handle_exc(exc)
                finally:
                    self.__ready = True
            return self.__ret

        def get_local(self):
            try:
                self.__ret = self.func(*self.args, **self.kwargs)
            except Exception as exc:
                self.__handle_exc(exc)
            finally:
                self.__ready = True
            return self.__ret

        @staticmethod
        def __fill_ph(items: Dict[Hashable, 'Workflow.Task'], key: Hashable,
                      handler: Callable = None, h_args: tuple = None, h_kwargs: dict = None):
            if key not in items:
                raise KeyError(key)
            else:
                ret = items[key].get()
                return ret if handler is None else handler(ret, *h_args, **h_kwargs)

        def fill_placeholders(self, items: Dict[Hashable, 'Workflow.Task'],
                              handler: Callable = None, h_args: tuple = None, h_kwargs: dict = None):
            if h_args is None:
                h_args = tuple()
            if h_kwargs is None:
                h_kwargs = dict()
            arg_list = list(self.args)
            for i, arg in enumerate(arg_list):
                if not isinstance(arg, Workflow.Placeholder):
                    continue
                arg_list[i] = Workflow.Task.__fill_ph(items, arg.name, handler, h_args, h_kwargs)
            self.args = tuple(arg_list)
            for k, v in self.kwargs.items():
                if not isinstance(v, Workflow.Placeholder):
                    continue
                self.kwargs[k] = Workflow.Task.__fill_ph(items, v.name, handler, h_args, h_kwargs)

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
        self.__ir = max(min(independent_ratio, 1.0), 0.0)
        self.__n_ind_workers = math.ceil(self.__n_workers * self.__ir)
        self.__n_dep_worders = self.__n_workers - self.__n_ind_workers
        self.__interval = max(check_interval, 0.001)
        self.__chain = DependencyChain()
        self.__tasks: Dict[Hashable, Workflow.Task] = dict()
        self.__1thd_pool = single_thread_pool
        # TODO: short circuit and broken circuit
        self.__short_c = list()
        self.__broken_c = list()

    def add(self, name: Hashable, task: Callable,
            args: tuple = None, kwargs: dict = None, after: Iterable[Hashable] = None,
            error: str = 'raise', coerce=None):
        self.__chain.add(name, after=after)
        self.__tasks[name] = Workflow.Task(task, args=args, kwargs=kwargs, error=error, coerce=coerce)

    @staticmethod
    def p(name):
        return Workflow.Placeholder(name)

    @staticmethod
    def __run_simple(pool: mp.Pool, tasks: Dict[Hashable, 'Workflow.Task'], interval: float):
        n_tasks = len(tasks)
        done_set = set()
        for name, task in tasks.items():
            task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        while len(done_set) != n_tasks:
            for name, task in tasks.items():
                if not task.ready():
                    continue
                task.get()
                done_set.add(name)
            time.sleep(interval)

    @staticmethod
    def __apply_ind(pool: mp.Pool, tasks: Dict[Hashable, 'Workflow.Task'],
                    ind_set: set, done_set: set, ind_running_set: set, n_ind_workers: int):
        if all([t in done_set for t in ind_set]):
            return
        ind_running_set -= done_set
        if len(ind_running_set) < n_ind_workers:
            for t in ind_set:
                task = tasks[t]
                if task.invoked():
                    continue
                task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
                ind_running_set.add(t)
                if len(ind_running_set) >= n_ind_workers:
                    break

    @staticmethod
    def __apply_bfs(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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
                    task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        return curr_level, level_tasks

    @staticmethod
    def __apply_dfs(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                    done_set: set, analyze: set):
        for item in analyze:  # dfs
            subs = chain.sub_of(item)
            for sub in subs:
                task = tasks[sub]
                s_sups = chain.sup_of(sub)
                if not task.invoked() and all([s_sup in done_set for s_sup in s_sups]):
                    task.fill_placeholders(tasks)
                    task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        analyze.clear()

    @staticmethod
    def __apply_dfs_recv(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
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
                        Workflow.__apply_dfs_recv(pool, chain, tasks, done_set, visit_set, ss_sup)
            if all([s_sup in done_set for s_sup in s_sups]):
                task.fill_placeholders(tasks)
                task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)

    @staticmethod
    def __run_dfs(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                  n_ind_workers: int, interval: float):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        ind_running_set = set()
        analyze = set()
        visit_set = set()

        apply_ind = Workflow.__apply_ind
        apply_dfs = Workflow.__apply_dfs_recv

        for t in chain[0]:
            task = tasks[t]
            task.fill_placeholders(tasks)
            task.result = pool.apply_async(task.func, args=task.args, kwds=task.kwargs)
        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, done_set, ind_running_set, n_ind_workers)  # independent tasks
            for item in analyze:  # dfs
                apply_dfs(pool, chain, tasks, done_set, visit_set, item)
            visit_set.clear()
            analyze.clear()
            for name, task in tasks.items():  # check status
                if not task.ready():
                    continue
                task.get()
                done_set.add(name)
                analyze.add(name)
            time.sleep(interval)

    @staticmethod
    def __run_bfs(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                  n_ind_workers: int, interval: float):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        curr_level = 0
        n_levels = len(chain)
        level_tasks = chain[curr_level]
        ind_running_set = set()

        apply_ind = Workflow.__apply_ind
        apply_bfs = Workflow.__apply_bfs

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, done_set, ind_running_set, n_ind_workers)  # independent tasks
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            for name, task in tasks.items():  # check status
                if not task.ready():
                    continue
                task.get()
                done_set.add(name)
            time.sleep(interval)

    @staticmethod
    def __run_mix(pool: mp.Pool, chain: DependencyChain, tasks: Dict[Hashable, 'Workflow.Task'],
                  n_ind_workers: int, interval: float):
        n_tasks = chain.size()
        done_set = set()
        ind_set = chain.independent_items()
        curr_level = 0
        n_levels = len(chain)
        level_tasks = chain[curr_level]
        ind_running_set = set()
        analyze = set()

        apply_ind = Workflow.__apply_ind
        apply_dfs = Workflow.__apply_dfs
        apply_bfs = Workflow.__apply_bfs

        while len(done_set) != n_tasks:
            apply_ind(pool, tasks, ind_set, done_set, ind_running_set, n_ind_workers)  # independent tasks
            apply_dfs(pool, chain, tasks, done_set, analyze)  # dfs
            curr_level, level_tasks = apply_bfs(pool, chain, tasks, level_tasks, done_set, curr_level, n_levels)  # bfs
            for name, task in tasks.items():  # check status
                if not task.ready():
                    continue
                task.get()
                done_set.add(name)
                analyze.add(name)
            time.sleep(interval)

    @staticmethod
    def __init_pool(processes: int, maxtasksperchild: int) -> mp.Pool:
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
        pool = Workflow.__init_pool(self.__n_workers, self.__maxtasksperchild)
        if n_level == 0:  # all independent tasks
            Workflow.__run_simple(pool, self.__tasks, self.__interval)
        else:
            if (m := self.__method) == 'mix':
                run_core = Workflow.__run_mix
            elif m == 'bfs':
                run_core = Workflow.__run_bfs
            elif m == 'dfs':
                run_core = Workflow.__run_dfs
            else:
                raise ValueError(f'unrecognized method "{m}"')
            run_core(pool, self.__chain, self.__tasks, self.__n_ind_workers, self.__interval)

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

    def run(self):
        invalid_set = self.__chain.invalid_items()
        if len(invalid_set) > 0:
            raise ValueError(f'invalid task(s) exists: {invalid_set}')
        if self.__chain.size() == 0:
            return

        if self.__n_workers == 1 and not self.__1thd_pool:
            self.__run_single()
        else:
            self.__run_parallel()
