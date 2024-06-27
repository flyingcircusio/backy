from abc import ABC, abstractmethod
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from structlog.stdlib import BoundLogger

if TYPE_CHECKING:
    from backy.repository import Repository
    from backy.revision import Revision

SOURCE_PLUGINS = entry_points(group="backy.sources")


def factory_by_type(type_) -> type["Source"]:
    return SOURCE_PLUGINS[type_].load()


class Source(ABC):
    """A source provides specific implementations for making and restoring
    backups.

    There are three major aspects provided by a source implementation:

    1. Extracting data from another system (e.g. Ceph RBD or S3).

    2. Storing that data in the repository directory.

    3. Restoring data, typically providing different workflows:

    - full restore into the original system (e.g. into an RBD image)
    - full restore into another system (e.g. into a local image file)
    - partial restore (e.g. allowing interactive access to a loop mounted version of the image)

    Additionally a few house keeping tasks need to be implemented:

    - garbage collection, to remove data that isn't needed after revisions
      have expired

    - verification of stored data to protect against low level corruption


    Implementations can be split into two parts:

    - a light shim as a Python class that can interact with the
      rest of the backy code within Python

    - a subprocess that backy interacts with to trigger the actual work.

    """

    type_: str
    subcommand: str

    repository: "Repository"

    def bind(self, repository: "Repository") -> None:
        self.repository = repository

    @classmethod
    @abstractmethod
    def from_config(cls, config: dict[str, Any], log: BoundLogger) -> "Source":
        ...

    @abstractmethod
    def backup(self, revision: "Revision") -> "Source":
        ...

    @abstractmethod
    def restore(self, revision: "Revision", *args, **kw) -> "Source":
        ...
