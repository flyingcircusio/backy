from abc import ABC, abstractmethod
from typing import IO, TYPE_CHECKING

from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from backy.backup import Backup
    from backy.revision import Revision


class BackendException(IOError):
    pass


class BackyBackend(ABC):
    @abstractmethod
    def __init__(self, revision: "Revision", log: BoundLogger) -> None:
        ...

    @abstractmethod
    def open(self, mode: str = "rb") -> IO:
        ...

    def purge(self) -> None:
        pass

    def verify(self) -> None:
        pass

    def scrub(self, backup: "Backup", type: str) -> None:
        pass
