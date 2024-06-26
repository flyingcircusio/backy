from pathlib import Path
from typing import Any

from structlog.stdlib import BoundLogger
from importlib.metadata import entry_points

SOURCE_PLUGINS = entry_points(group='backy.sources')

def factory_by_type(type_):
    return SOURCE_PLUGINS[type_].load()


class Source:

    type_: str
    config: dict[str, Any]

    @classmethod
    def init(cls, repository: Path, log: BoundLogger) -> dict[str, Any]:
        return {"type": cls.type_}
