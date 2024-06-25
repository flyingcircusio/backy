from abc import ABC, abstractmethod
from typing import IO, TYPE_CHECKING, Optional, Type

from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from backy.revision import Revision


class BackendException(IOError):
    pass


class BackyBackend(ABC):
    @abstractmethod
    def __init__(self, revision: "Revision", log: BoundLogger) -> None:
        ...

    @abstractmethod
    def open(self, mode: str = "rb", parent: Optional["Revision"] = None) -> IO:
        ...

    def purge(self) -> None:
        pass

    def verify(self) -> None:
        pass


def select_backend(type_: str) -> Type[BackyBackend]:
    match type_:
        case "chunked":
            from backy.backends.chunked import ChunkedFileBackend

            return ChunkedFileBackend
        case _:
            raise ValueError(f"Invalid backend '{type_}'")
