import datetime
from enum import Enum
from pathlib import Path
from typing import IO, TYPE_CHECKING, Literal, Optional

import shortuuid
import yaml
from structlog.stdlib import BoundLogger

from . import utils
from .backends import select_backend
from .utils import SafeFile

if TYPE_CHECKING:
    from .backends import BackyBackend
    from .backup import Backup


TAG_MANUAL_PREFIX = "manual:"


class Trust(Enum):
    TRUSTED = "trusted"
    DISTRUSTED = "distrusted"
    VERIFIED = "verified"


def filter_schedule_tags(tags):
    return {t for t in tags if not t.startswith(TAG_MANUAL_PREFIX)}


def filter_manual_tags(tags):
    return {t for t in tags if t.startswith(TAG_MANUAL_PREFIX)}


class Revision(object):
    backup: "Backup"
    uuid: str
    timestamp: datetime.datetime
    stats: dict
    tags: set[str]
    trust: Trust = Trust.TRUSTED
    backend_type: Literal["cowfile", "chunked"] = "chunked"
    log: BoundLogger

    def __init__(
        self,
        backup: "Backup",
        log: BoundLogger,
        uuid: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> None:
        self.backup = backup
        self.uuid = uuid if uuid else shortuuid.uuid()
        self.timestamp = timestamp if timestamp else utils.now()
        self.stats = {"bytes_written": 0}
        self.tags = set()
        self.log = log.bind(revision_uuid=self.uuid, subsystem="revision")

    @classmethod
    def create(
        cls,
        backup: "Backup",
        tags: set[str],
        log: BoundLogger,
        *,
        uuid: Optional[str] = None,
    ) -> "Revision":
        r = Revision(backup, log, uuid)
        r.tags = tags
        r.backend_type = backup.default_backend_type
        return r

    @property
    def backend(self) -> "BackyBackend":
        return select_backend(self.backend_type)(self, self.log)

    def open(self, mode: str = "rb") -> IO:
        return self.backend.open(mode)

    @classmethod
    def load(cls, file: Path, backup: "Backup", log: BoundLogger) -> "Revision":
        with file.open(encoding="utf-8") as f:
            metadata = yaml.safe_load(f)
        assert metadata["timestamp"].tzinfo == datetime.timezone.utc
        r = Revision(
            backup, log, uuid=metadata["uuid"], timestamp=metadata["timestamp"]
        )
        r.stats = metadata.get("stats", {})
        r.tags = set(metadata.get("tags", []))
        # Assume trusted by default to support migration
        r.trust = Trust(metadata.get("trust", Trust.TRUSTED.value))
        # If the metadata does not show the backend type, then it's cowfile.
        r.backend_type = metadata.get("backend_type", "cowfile")
        return r

    @property
    def filename(self) -> Path:
        """Full pathname of the image file."""
        return self.backup.path / self.uuid

    @property
    def info_filename(self) -> Path:
        """Full pathname of the metadata file."""
        return self.filename.with_suffix(self.filename.suffix + ".rev")

    def materialize(self) -> None:
        self.write_info()
        self.writable()

    def write_info(self) -> None:
        self.log.debug("writing-info", tags=", ".join(self.tags))
        with SafeFile(self.info_filename, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.safe_dump(self.to_dict(), f)

    def to_dict(self) -> dict:
        return {
            "uuid": self.uuid,
            "backend_type": self.backend_type,
            "timestamp": self.timestamp,
            "parent": getattr(
                self.get_parent(), "uuid", ""
            ),  # compatibility with older versions
            "stats": self.stats,
            "trust": self.trust.value,
            "tags": list(self.tags),
        }

    def distrust(self) -> None:
        self.log.info("distrusted")
        self.trust = Trust.DISTRUSTED

    def verify(self) -> None:
        self.log.info("verified")
        self.trust = Trust.VERIFIED

    def remove(self) -> None:
        self.log.info("remove")
        for filename in self.filename.parent.glob(self.filename.name + "*"):
            if filename.exists():
                self.log.debug("remove-start", filename=filename)
                filename.unlink()
                self.log.debug("remove-end", filename=filename)

        if self in self.backup.history:
            self.backup.history.remove(self)

    def writable(self) -> None:
        if self.filename.exists():
            self.filename.chmod(0o640)
        self.info_filename.chmod(0o640)

    def readonly(self) -> None:
        if self.filename.exists():
            self.filename.chmod(0o440)
        self.info_filename.chmod(0o440)

    def get_parent(self) -> Optional["Revision"]:
        """defaults to last rev if not in history"""
        prev = None
        for r in self.backup.history:
            if r.backend_type != self.backend_type:
                continue
            if r.uuid == self.uuid:
                break
            prev = r
        return prev
