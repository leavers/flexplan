from typing import (
    Any, Callable, ClassVar, Dict, Hashable, List, Literal,
    Optional, Set, Tuple, TypeVar, Union
)

import atexit
import os
import time
import threading
import itertools
import weakref
import socket
import selectors

from weakref import ReferenceType
from dataclasses import dataclass, field
import multiprocessing as mp

import queue

T = TypeVar('T')
TaskMode = Literal['thread', 'process', 'async']

MODE_THREAD = 'thread'
MODE_PROCESS = 'process'
MODE_ASYNC = 'async'

_manager_threads = weakref.WeakSet()
_global_shutdown = False


@atexit.register
def _python_exit():
    global _global_shutdown
    _global_shutdown = True
    for t in _manager_threads:
        t.join()


class Future:
    def __init__(self):
        self._result: Any = None
        self._exc: Optional[BaseException] = None
        self._event: threading.Event = threading.Event()

    def ready(self):
        return self._event.is_set()

    def get(self):
        self._event.wait()
        if self._exc:
            raise self._exc
        return self._result

    result = get

    def set_result(
            self,
            result: Any = None,
            exception: BaseException = None
    ):
        self._result = result
        self._exc = exception
        self._event.set()


class PollQueue(queue.SimpleQueue):
    def __init__(self):
        super().__init__()
        self._put_s, self._get_s = socket.socketpair()

    @property
    def sentinel(self):
        return self._get_s.fileno()

    def put(self, item, block: bool = True, timeout: float = None):
        super().put(item, block, timeout)
        self._put_s.send(b'1')

    def get(self, block: bool = True, timeout: float = None):
        ret = super().get(block, timeout)
        self._get_s.recv(1)
        return ret


ACT_NONE = 0
ACT_DONE = 1
ACT_EXCEPTION = 2
ACT_CLOSE = 3
ACT_RESTART = 4
ACT_RESET = 5
ACT_CANCEL = 6
ACT_COERCE = 7
ACT_RUNNING = 8
ACT_EXCEPTION_RESTART = 9
ACT_TIMEOUT_CLOSE = 10

ACT_RUNNING_FLAGS = (ACT_RUNNING,)
ACT_DONE_FLAGS = (ACT_DONE, ACT_EXCEPTION, ACT_EXCEPTION_RESTART)  # used for manager
ACT_IDLE_FLAGS = (ACT_NONE, ACT_DONE, ACT_EXCEPTION)  # used for manager
ACT_RESTART_FLAGS = (ACT_RESTART, ACT_EXCEPTION_RESTART)  # used for manager
ACT_EXIT_FLAGS = (ACT_CLOSE, ACT_RESTART, ACT_EXCEPTION_RESTART, ACT_TIMEOUT_CLOSE)  # used for manager/worker


@dataclass
class _WorkItem:
    name: Hashable
    future: Future
    func: Callable[..., Any]
    args: Tuple[Any] = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _ThreadWorkerContext:
    name: Hashable
    work_queue: PollQueue
    request_queue: PollQueue
    response_queue: PollQueue
    daemon: bool = True
    idle_timeout: float = 60.0
    wait_timeout: float = 0.1
    max_work_count: int = 12
    max_err_count: int = 3
    max_cons_err_count: int = -1


@dataclass
class _ActionItem:
    action: int = ACT_NONE
    work_id: Hashable = None
    worker_id: Hashable = None
    result: Any = None
    exception: BaseException = None
    settings: Any = None


class _ThreadWorker:
    def __init__(
            self,
            ctx: _ThreadWorkerContext
    ):
        self._work_queue = ctx.work_queue
        self._name = ctx.name
        self._daemon: bool = ctx.daemon
        self._wait_timeout: float = ctx.wait_timeout
        self.ctx = ctx

        self._running: bool = False
        self._exit: bool = False
        self._thread: Optional[threading.Thread] = None

        self._idle: bool = True
        self._work_id: Optional[Hashable] = None

    def _init_response_ctx(self, work_id: Hashable = None):
        self._idle = False
        self._work_id = work_id

    def _exit_response_ctx(self):
        self._idle = True
        self._work_id = None

    def _get_response(
            self,
            action: int = ACT_NONE,
            result: Any = None,
            exception: BaseException = None
    ) -> _ActionItem:
        return _ActionItem(action=action,
                           work_id=self._work_id,
                           worker_id=self._name,
                           result=result,
                           exception=exception)

    def run(self):
        ctx: _ThreadWorkerContext = self.ctx
        work_queue: PollQueue = ctx.work_queue
        request_queue: PollQueue = ctx.request_queue
        response_queue: PollQueue = ctx.response_queue
        idle_timeout: float = self.ctx.idle_timeout
        wait_timeout: float = self.ctx.wait_timeout

        get_response = self._get_response
        init_response_ctx = self._init_response_ctx
        exit_response_ctx = self._exit_response_ctx

        work_count: int = 0
        err_count: int = 0
        cons_err_count: int = 0

        response: Optional[_ActionItem] = None
        self._running = True
        idle_tick: float = time.monotonic()
        while True:
            if time.monotonic() - idle_tick > idle_timeout:
                response = get_response(action=ACT_CLOSE)
                print(f'Worker {self._name} idle timeout')
                break
            while not request_queue.empty():
                request: _ActionItem = request_queue.get()
                if request.action in ACT_EXIT_FLAGS:
                    response = get_response(action=request.action)
                    self._exit = True
                    break
                elif request.action == ACT_RESET:
                    work_count = 0
                    err_count = 0
                    cons_err_count = 0
                else:
                    raise RuntimeError(f'Unknown command flag: {request.action}')
            if self._exit:
                break
            try:  # Wait for new work to execute
                work_item: _WorkItem = work_queue.get(timeout=wait_timeout)
            except queue.Empty:
                continue
            result = None
            try:
                init_response_ctx(work_item.name)
                result = work_item.func(*work_item.args, **work_item.kwargs)
                cons_err_count = 0
                response_queue.put(get_response(action=ACT_DONE, result=result))
            except BaseException as exc:
                err_count += 1
                cons_err_count += 1
                response = get_response(action=ACT_EXCEPTION, result=result, exception=exc)
                if 0 <= ctx.max_err_count < err_count \
                        or 0 <= ctx.max_cons_err_count < cons_err_count:
                    response.action = ACT_EXCEPTION_RESTART
                    break
                response_queue.put(response)
            finally:
                work_count += 1
                exit_response_ctx()
                idle_tick: float = time.monotonic()
                if 0 <= ctx.max_work_count <= work_count:
                    response = get_response(action=ACT_RESTART)
                    break
        if response is not None and response.action != ACT_NONE:
            self._running = False
            response_queue.put(response)

    def idle(self) -> bool:
        return self._idle

    def start(self):
        if self._running:
            raise RuntimeError('Thread is already running')
        self._thread = threading.Thread(
            target=self.run,
            daemon=self._daemon
        )
        self._thread.start()

    def stop(self):
        if not self._running:
            return
        if self._thread is not None:
            self._exit = True
            self._thread.join()


def _worker_manager(
        executor_ref: ReferenceType = None,
        thread_workers: Dict[Hashable, _ThreadWorker] = None,
        worker_items: Dict[Hashable, _WorkItem] = None,
        work_queue: PollQueue = None,
        response_queue: PollQueue = None,
        stop_event: threading.Event = None,
        max_thread_workers: int = 4,
        thread_worker_id_prefix: str = None,
        incremental_thread_pool: bool = True,
        select_timeout: float = 0.1,
        max_thread_actions: int = 10,
        max_process_actions: int = 10
):
    # TODO: Make max_thread_actions and max_process_actions more dynamic based on number of work/workers/cpu_count.

    next_thread_worker_id = itertools.count().__next__
    static_thread_pool: bool = False

    def get_next_worker_id():
        while (id_ := f'{thread_worker_id_prefix}{next_thread_worker_id()}') not in thread_workers:
            return id_

    def get_default_ctx(id_: str):
        return _ThreadWorkerContext(
            name=id_,
            work_queue=work_queue,
            request_queue=PollQueue(),
            response_queue=response_queue,
            daemon=True,
            idle_timeout=60.0,
            wait_timeout=0.1,
            max_work_count=12,
            max_err_count=3,
            max_cons_err_count=-1
        )

    def adjust_thread_workers():
        nonlocal static_thread_pool
        curr_workers = len(thread_workers)
        if static_thread_pool or curr_workers == max_thread_workers:
            return
        if incremental_thread_pool or max_thread_workers < 0:
            idle_workers: int = sum(1 if w.idle() else 0 for w in thread_workers.values())
            qsize: int = work_queue.qsize()
            if max_thread_workers < 0:
                iterator = range(qsize - idle_workers)
            else:
                iterator = range(curr_workers, min(max_thread_workers, curr_workers + qsize - idle_workers))
            for _ in iterator:
                id_ = get_next_worker_id()
                print(f'Generate new worker {id_}')
                thread_worker = _ThreadWorker(get_default_ctx(id_))
                thread_workers[id_] = thread_worker
                thread_worker.start()
        else:
            for _ in range(max_thread_workers):
                id_ = get_next_worker_id()
                thread_worker = _ThreadWorker(get_default_ctx(id_))
                thread_workers[id_] = thread_worker
                thread_worker.start()
            static_thread_pool = True

    if hasattr(selectors, 'PollSelector'):
        _Selector = selectors.PollSelector
    else:
        _Selector = selectors.SelectSelector
    selector = _Selector()
    selector.register(response_queue.sentinel, selectors.EVENT_READ)

    def select():
        ready = selector.select(select_timeout)
        return True if ready else False

    def stop_workers():
        stop_action = _ActionItem(action=ACT_CLOSE)
        for _, worker in thread_workers.items():
            worker.ctx.request_queue.put(stop_action)
        for _, worker in thread_workers.items():
            worker.stop()

    while True:
        if stop_event.is_set():
            break
        executor: Optional[_ThreadPoolExecutor] = executor_ref()
        if executor is None:
            break
        if _global_shutdown:
            break
        adjust_thread_workers()
        if not select():
            continue
        thread_actions_count = 0
        while not response_queue.empty():
            response: _ActionItem = response_queue.get()
            action = response.action
            work_id = response.work_id
            worker_id = response.worker_id
            if action in ACT_DONE_FLAGS:
                work_item = worker_items.pop(work_id)  # get and delete the work from worker_items
                work_item.future.set_result(response.result, response.exception)
            if action in ACT_RESTART_FLAGS:
                thread_workers[worker_id].start()
            elif action == ACT_CLOSE:
                thread_workers.pop(worker_id)
            thread_actions_count += 1
            if thread_actions_count >= max_thread_actions:
                break
    stop_workers()


class _ThreadPoolExecutor:
    _next_work_seq = itertools.count().__next__
    _next_worker_seq = itertools.count().__next__

    def __init__(
            self,
            workers: int = None,
            incremental_thread_pool: bool = True,
            work_name_prefix: str = 'Work-',
            thread_worker_name_prefix: str = 'ThreadWorker-'
    ):
        if workers in (None, 0):
            self._max_thread_workers = min(32, (os.cpu_count() or 1) + 4)
        elif workers < 0:  # Unlimited
            self._max_thread_workers = -1
        self._incremental_thread_pool = incremental_thread_pool
        self._work_name_prefix = work_name_prefix
        self._thread_worker_id_prefix = thread_worker_name_prefix

        self._work_queue = PollQueue()
        self._work_items: Dict[Hashable, _WorkItem] = {}
        self._thread_workers: Dict[Hashable, _ThreadWorker] = {}
        self._response_queue = PollQueue()
        self._management_thread: Optional[threading.Thread] = None

        self._stop_event: threading.Event = threading.Event()

    def submit(
            self,
            func: Callable[..., Any],
            args: Tuple[Any] = (),
            kwargs: Dict[str, Any] = None,
            name: Hashable = None
    ) -> Future:
        if kwargs is None:
            kwargs = {}
        if name is None:
            while (n := f'{self._work_name_prefix}{_ThreadPoolExecutor._next_work_seq()}') not in self._work_items:
                name = n
                break
        elif name in self._work_items:
            raise KeyError(f'Work name "{name}" exists')
        _, future = self._gen_work_item_and_future(func, args, kwargs, name)
        self._wakeup_worker_manager()

        return future

    def _gen_work_item_and_future(
            self,
            func: Callable[..., Any],
            args: Tuple[Any],
            kwargs: Dict[str, Any],
            name: Hashable
    ) -> Tuple[_WorkItem, Future]:
        future = Future()
        work_item = _WorkItem(name, future, func, args, kwargs)
        self._work_items[name] = work_item
        self._work_queue.put(work_item)
        return work_item, future

    def _wakeup_worker_manager(self):
        if self._management_thread is None:
            def shutdown_cb(executor_ref: ReferenceType):
                executor = executor_ref()
                if executor:
                    executor.shutdown()

            self._management_thread = threading.Thread(
                target=_worker_manager,
                name='ThreadManager',
                kwargs={
                    'executor_ref': weakref.ref(self, shutdown_cb),
                    'thread_workers': self._thread_workers,
                    'worker_items': self._work_items,
                    'work_queue': self._work_queue,
                    'response_queue': self._response_queue,
                    'stop_event': self._stop_event,
                    'max_thread_workers': self._max_thread_workers,
                    'thread_worker_id_prefix': self._thread_worker_id_prefix,
                    'incremental_thread_pool': self._incremental_thread_pool,
                },
                daemon=True
            )
            self._management_thread.start()
            _manager_threads.add(self._management_thread)

    def shutdown(self):
        if self._management_thread is not None:
            self._stop_event.set()
            self._management_thread.join()
            self._management_thread = None


def func(t: float):
    print(f'Func {t} starts')
    time.sleep(t)
    print(f'Func {t} ends')
    return t


if __name__ == '__main__':
    count = 36
    executor = _ThreadPoolExecutor(workers=-1)
    futures = []
    for i in range(count):
        futures.append(executor.submit(func=func, args=(5,)))
    for future in futures:
        future.get()
