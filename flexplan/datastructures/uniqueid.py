from uuid import UUID, uuid4

import typing_extensions as t

__all__ = (
    "UniqueID",
    "UUID",
    "uuid4",
)


UniqueID = t.Union[UUID, str]
