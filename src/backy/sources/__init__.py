from abc import ABC, abstractmethod
from typing import Type

import pkg_resources
from structlog.stdlib import BoundLogger

import backy.backends
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
    entry_points = list(pkg_resources.iter_entry_points("backy.sources", type_))
    return entry_points[0].load()
