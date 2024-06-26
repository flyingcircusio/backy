from pathlib import Path
from typing import Any

from structlog.stdlib import BoundLogger

from backy.file import FileSource

# XXX Use plugin discovery here
KNOWN_SOURCES: dict[str, "Source"] = {s.type_: s for s in [FileSource]}


class Source:

    type_: str
    config: dict[str, Any]

    @classmethod
    def init(cls, repository: Path, log: BoundLogger) -> dict[str, Any]:
        return {"type": cls.type_}
