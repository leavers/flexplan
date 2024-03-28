class FlexplanError(Exception):
    ...


class WorkerNotFoundError(FlexplanError):
    ...


class WorkerRuntimeError(FlexplanError):
    ...
