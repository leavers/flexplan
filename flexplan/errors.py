class FlexplanError(Exception):
    ...


class ArgumentError(FlexplanError):
    ...


class ArgumentTypeError(ArgumentError, TypeError):
    ...


class ArgumentValueError(ArgumentError, ValueError):
    ...


class WorkerNotFoundError(FlexplanError):
    ...


class WorkerRuntimeError(FlexplanError):
    ...
