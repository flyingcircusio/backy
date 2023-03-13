import datetime
import glob
import os
import os.path as p
from enum import Enum
from typing import Optional

import shortuuid
import yaml
from structlog.stdlib import BoundLogger

import backy.backup

from .utils import SafeFile, now, safe_symlink

TAG_MANUAL_PREFIX = "manual:"


class Trust(Enum):
    TRUSTED = "trusted"
    DISTRUSTED = "distrusted"
    VERIFIED = "verified"


def filter_schedule_tags(tags):
    return {t for t in tags if not t.startswith(TAG_MANUAL_PREFIX)}


class Revision(object):
    backup: "backy.backup.Backup"
    uuid: str
    timestamp: datetime.datetime
    parent: Optional[str] = None
    stats: dict
    tags: set[str]
    trust: Trust = Trust.TRUSTED
    backend_type: str = "chunked"
    log: BoundLogger

    def __init__(self, backup, log, uuid=None, timestamp=None):
        self.backup = backup
        self.uuid = uuid if uuid else shortuuid.uuid()
        self.timestamp = timestamp if timestamp else now()
        self.stats = {"bytes_written": 0}
        self.tags = set()
        self.log = log.bind(revision_uuid=self.uuid, subsystem="revision")

    @classmethod
    def create(cls, backup, tags, log):
        r = Revision(backup, log)
        r.tags = tags
        if backup.history:
            r.parent = backup.history[-1].uuid
        r.backend_type = backup.backend_type
        return r

    @property
    def backend(self):
        return self.backup.backend_factory(self, self.log)

    def open(self, mode="rb"):
        return self.backend.open(mode)

    @classmethod
    def load(cls, filename, backup, log):
        with open(filename, encoding="utf-8") as f:
            metadata = yaml.safe_load(f)
        assert metadata["timestamp"].tzinfo == datetime.timezone.utc
        r = Revision(
            backup, log, uuid=metadata["uuid"], timestamp=metadata["timestamp"]
        )
        r.parent = metadata["parent"]
        r.stats = metadata.get("stats", {})
        r.tags = set(metadata.get("tags", []))
        # Assume trusted by default to support migration
        r.trust = Trust(metadata.get("trust", Trust.TRUSTED.value))
        # If the metadata does not show the backend type, then it's cowfile.
        r.backend_type = metadata.get("backend_type", "cowfile")
        return r

    @property
    def filename(self):
        """Full pathname of the image file."""
        return os.path.join(self.backup.path, str(self.uuid))

    @property
    def info_filename(self):
        """Full pathname of the metadata file."""
        return self.filename + ".rev"

    def materialize(self):
        self.write_info()
        self.writable()

    def write_info(self):
        self.log.debug("writing-info", tags=", ".join(self.tags))
        metadata = {
            "uuid": self.uuid,
            "backend_type": self.backend_type,
            "timestamp": self.timestamp,
            "parent": self.parent,
            "stats": self.stats,
            "trust": self.trust.value,
            "tags": list(self.tags),
        }
        with SafeFile(self.info_filename, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.safe_dump(metadata, f)

    def set_link(self, name):
        path = self.backup.path
        safe_symlink(self.filename, p.join(path, name))
        safe_symlink(self.info_filename, p.join(path, name + ".rev"))

    def distrust(self):
        self.log.info("distrusted")
        self.trust = Trust.DISTRUSTED

    def verify(self):
        self.log.info("verified")
        self.trust = Trust.VERIFIED

    def remove(self):
        self.log.info("remove")
        for filename in glob.glob(self.filename + "*"):
            if os.path.exists(filename):
                self.log.debug("remove-start", filename=filename)
                os.remove(filename)
                self.log.debug("remove-end", filename=filename)

        if self in self.backup.history:
            self.backup.history.remove(self)

    def writable(self):
        if os.path.exists(self.filename):
            os.chmod(self.filename, 0o640)
        os.chmod(self.info_filename, 0o640)

    def readonly(self):
        if os.path.exists(self.filename):
            os.chmod(self.filename, 0o440)
        os.chmod(self.info_filename, 0o440)

    def get_parent(self):
        if self.parent:
            return self.backup.find_by_uuid(self.parent)
        return
