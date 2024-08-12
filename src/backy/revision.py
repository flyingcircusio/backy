import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import shortuuid
import yaml
from structlog.stdlib import BoundLogger

from . import utils
from .utils import SafeFile

if TYPE_CHECKING:
    from .repository import Repository


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
    repository: "Repository"
    uuid: str
    timestamp: datetime.datetime
    stats: dict
    tags: set[str]
    orig_tags: set[str]
    trust: Trust = Trust.TRUSTED
    server: str = ""
    log: BoundLogger

    def __init__(
        self,
        repository: "Repository",
        log: BoundLogger,
        uuid: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> None:
        self.repository = repository
        self.uuid = uuid if uuid else shortuuid.uuid()
        self.timestamp = timestamp if timestamp else utils.now()
        self.stats = {"bytes_written": 0}
        self.tags = set()
        self.orig_tags = set()
        self.log = log.bind(revision_uuid=self.uuid, subsystem="revision")

    @classmethod
    def create(
        cls,
        repository: "Repository",
        tags: set[str],
        log: BoundLogger,
        *,
        uuid: Optional[str] = None,
    ) -> "Revision":
        r = Revision(repository, log, uuid)
        r.tags = tags
        return r

    @classmethod
    def load(
        cls, file: Path, backup: "Repository", log: BoundLogger
    ) -> "Revision":
        with file.open(encoding="utf-8") as f:
            metadata = yaml.safe_load(f)
        r = cls.from_dict(metadata, backup, log)
        return r

    @classmethod
    def from_dict(cls, metadata, backup, log):
        ts = metadata["timestamp"]
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)
        assert ts.tzinfo == datetime.timezone.utc
        r = Revision(backup, log, uuid=metadata["uuid"], timestamp=ts)
        r.stats = metadata.get("stats", {})
        r.tags = set(metadata.get("tags", []))
        r.orig_tags = set(metadata.get("orig_tags", []))
        r.server = metadata.get("server", "")
        # Assume trusted by default to support migration
        r.trust = Trust(metadata.get("trust", Trust.TRUSTED.value))
        return r

    @property
    def info_filename(self) -> Path:
        """Full pathname of the metadata file."""
        p = self.repository.path / self.uuid
        return p.with_suffix(p.suffix + ".rev")

    def materialize(self) -> None:
        self.write_info()
        self.writable()

    def write_info(self) -> None:
        self.log.debug("writing-info", tags=", ".join(self.tags))
        with SafeFile(self.info_filename, encoding="utf-8") as f:
            f.open_new("wb")
            f.write("# Please use the `backy tags` subcommand to edit tags\n")
            yaml.safe_dump(self.to_dict(), f)

    def to_dict(self) -> dict:
        return {
            "uuid": self.uuid,
            "timestamp": self.timestamp,
            "parent": getattr(
                self.get_parent(), "uuid", ""
            ),  # compatibility with older versions
            "stats": self.stats,
            "trust": self.trust.value,
            "tags": list(self.tags),
            "orig_tags": list(self.orig_tags),
            "server": self.server,
        }

    # TODO: disallow local modification
    @property
    def pending_changes(self):
        return self.server and self.tags != self.orig_tags

    def distrust(self) -> None:
        assert not self.server
        self.log.info("distrusted")
        self.trust = Trust.DISTRUSTED

    def verify(self) -> None:
        assert not self.server
        self.log.info("verified")
        self.trust = Trust.VERIFIED

    def remove(self, force=False) -> None:
        self.log.info("remove")
        if not force and self.server:
            self.log.debug("remove-remote", server=self.server)
            self.tags = set()
            self.write_info()
        else:
            if self.info_filename.exists():
                self.log.debug("remove-start", filename=str(self.info_filename))
                self.info_filename.unlink()
                self.log.debug("remove-end", filename=str(self.info_filename))

            if self in self.repository.history:
                self.repository.history.remove(self)
                del self.repository._by_uuid[self.uuid]

    def writable(self) -> None:
        self.info_filename.chmod(0o640)

    def readonly(self) -> None:
        self.info_filename.chmod(0o440)

    def get_parent(self, ignore_trust=False) -> Optional["Revision"]:
        """defaults to last rev if not in history"""
        prev = None
        for r in self.repository.history:
            if not ignore_trust and r.trust == Trust.DISTRUSTED:
                continue
            if r.server != self.server:
                continue
            if r.uuid == self.uuid:
                break
            prev = r
        return prev
