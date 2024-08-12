import datetime
import hashlib
import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import shortuuid
import yaml
from structlog.stdlib import BoundLogger
from yaml import SafeDumper

import backy.utils
from backy.utils import SafeFile


class ProblemReport(ABC):
    uuid: str
    timestamp: datetime.datetime

    def __init__(
        self,
        uuid: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
    ):
        self.uuid = uuid or shortuuid.uuid()
        self.timestamp = timestamp or backy.utils.now()

    def to_dict(self) -> dict:
        return {
            "uuid": self.uuid,
            "timestamp": self.timestamp,
        }

    @abstractmethod
    def get_message(self) -> str:
        ...

    def store(self, dir: Path, log: BoundLogger) -> None:
        log.debug("store-report", uuid=self.uuid)
        path = dir / f"{self.uuid}.report"
        if path.exists():
            log.debug("store-report-exists", uuid=self.uuid)
            return

        class CustomDumper(SafeDumper):
            pass

        def representer(dumper, data):
            return dumper.represent_scalar(
                "tag:yaml.org,2002:str",
                data,
                style="|" if len(data) > 100 else None,
            )

        yaml.add_representer(str, representer, Dumper=CustomDumper)

        with SafeFile(path, encoding="utf-8") as f:
            f.open_new("wb")
            yaml.dump(self.to_dict(), f, sort_keys=False, Dumper=CustomDumper)


class ChunkMismatchReport(ProblemReport):
    source_chunk: bytes
    source_hash: str
    target_chunk: bytes
    target_hash: str
    offset: int
    traceback: str

    def __init__(self, source_chunk: bytes, target_chunk: bytes, offset: int):
        super().__init__()
        self.source_chunk = source_chunk
        self.target_chunk = target_chunk
        self.offset = offset
        self.source_hash = hashlib.md5(self.source_chunk).hexdigest()
        self.target_hash = hashlib.md5(self.target_chunk).hexdigest()
        self.traceback = "".join(traceback.format_stack()).strip()

    def to_dict(self) -> dict:
        return super().to_dict() | {
            "source_hash": self.source_hash,
            "target_hash": self.target_hash,
            "offset": self.offset,
            "traceback": self.traceback,
        }

    def get_message(self) -> str:
        return f"Mismatching chunks at offset {self.offset}"

    def store(self, dir: Path, log: BoundLogger) -> None:
        chunks_path = dir / "chunks"
        chunks_path.mkdir(exist_ok=True)
        self._store_chunk(chunks_path, self.source_chunk, self.source_hash, log)
        self._store_chunk(chunks_path, self.target_chunk, self.target_hash, log)
        super().store(dir, log)

    @staticmethod
    def _store_chunk(
        dir: Path, chunk: bytes, hash: str, log: BoundLogger
    ) -> None:
        log.debug("store-chunk", hash=hash)
        path = dir / hash
        if path.exists():
            log.debug("store-chunk-exists", hash=hash)
            return
        with SafeFile(path) as f:
            f.open_new("wb")
            f.write(chunk)
