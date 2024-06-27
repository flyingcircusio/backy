from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

from structlog.stdlib import BoundLogger

SOURCE_PLUGINS = entry_points(group="backy.sources")


def factory_by_type(type_):
    return SOURCE_PLUGINS[type_].load()


class Source:
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
    config: dict[str, Any]

    @classmethod
    def init(cls, repository: Path, log: BoundLogger) -> dict[str, Any]:
        return {"type": cls.type_}
