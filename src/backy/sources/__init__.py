from abc import ABC, abstractmethod
from importlib.metadata import entry_points
from typing import Type

from structlog.stdlib import BoundLogger

import backy.revision


class BackySource(ABC):
    @abstractmethod
    def backup(self, target: "backy.backends.BackyBackend") -> None:
        ...

    @abstractmethod
    def verify(self, target: "backy.backends.BackyBackend") -> bool:
        ...


class BackySourceContext(ABC):
    @abstractmethod
    def __enter__(self) -> BackySource:
        ...

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        pass


class BackySourceFactory(ABC):
    @abstractmethod
    def __init__(self, config: dict, log: BoundLogger) -> None:
        ...

    @abstractmethod
    def __call__(
        self, revision: "backy.revision.Revision"
    ) -> BackySourceContext:
        ...

    @abstractmethod
    def ready(self) -> bool:
        """Check whether the source can be backed up."""
        ...


def select_source(type_: str) -> Type[BackySourceFactory]:
    match type_:
        case "flyingcircus":
            from backy.sources.flyingcircus.source import FlyingCircusRootDisk

            return FlyingCircusRootDisk
        case "ceph-rbd":
            from backy.sources.ceph.source import CephRBD

            return CephRBD
        case "file":
            from backy.sources.file import File

            return File
        case _:
            raise ValueError(f"invalid backend: {type_}")
