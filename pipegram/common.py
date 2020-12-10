from typing import Hashable, List, Literal, Set, Union

hashable_types = (str, int, float, tuple, bytes, bool, complex)

Dependent = Union[Hashable, List[Hashable], Set[Hashable]]

MAX_THREADS = 64
MAX_PROCESSES = 32

WORK_MODE_THREAD = 'thread'
WORK_MODE_PROCESS = 'process'
WORK_MODE_ASYNC = 'async'
WORK_MODE_LOCAL = 'local'
work_modes = (WORK_MODE_THREAD, WORK_MODE_PROCESS, WORK_MODE_ASYNC, WORK_MODE_LOCAL)
WorkMode = Literal['thread', 'process', 'async', 'local']
ThreadFallbackMode = Literal['process', 'local']
ProcessFallbackMode = Literal['thread', 'local']
AsyncFallbackMode = Literal['thread', 'local']

WORKFLOW_MODE_MIX = 'mix'
WORKFLOW_MODE_BFS = 'bfs'
WORKFLOW_MODE_DFS = 'dfs'
workflow_modes = (WORKFLOW_MODE_MIX, WORKFLOW_MODE_BFS, WORKFLOW_MODE_DFS)
WorkflowMode = Literal['mix', 'bfs', 'dfs']

ERROR_MODE_RAISE = 'raise'
ERROR_MODE_IGNORE = 'ignore'
ERROR_MODE_COERCE = 'coerce'
error_modes = (ERROR_MODE_RAISE, ERROR_MODE_IGNORE, ERROR_MODE_COERCE)
ErrorMode = Literal['raise', 'ignore', 'coerce']


class SignatureNotMatchError(Exception):
    pass
