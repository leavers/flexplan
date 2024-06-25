from uuid import uuid4

from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from flexplan.types import WorkerId


def gen_worker_id() -> "WorkerId":
    return str(uuid4())
