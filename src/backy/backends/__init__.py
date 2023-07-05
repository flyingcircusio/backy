from abc import ABC, abstractmethod

from structlog.stdlib import BoundLogger

import backy.backup
import backy.revision


class BackendException(IOError):
    pass


class BackyBackend(ABC):
    @abstractmethod
    def __init__(
        self, revision: "backy.revision.Revision", log: BoundLogger
    ) -> None:
        ...

    @abstractmethod
    def open(self, mode="rb"):
        ...

    def purge(self) -> None:
        pass

    def verify(self) -> None:
        pass

    def scrub(self, backup: "backy.backup.Backup", type: str) -> None:
        pass
