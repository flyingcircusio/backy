import dataclasses
import datetime
import hashlib
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import shortuuid
import yaml
from structlog.stdlib import BoundLogger
from yaml import SafeDumper

import backy
from backy.utils import SafeFile


@dataclass(frozen=True)
class ProblemReport:
    uuid: str = field(init=False, default_factory=shortuuid.uuid)
    timestamp: datetime.datetime = field(
        init=False, default_factory=backy.utils.now
    )

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

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


@dataclass(frozen=True)
class ChunkMismatchReport(ProblemReport):
    source_chunk: bytes
    source_hash: str = field(init=False)
    target_chunk: bytes
    target_hash: str = field(init=False)
    offset: int
    traceback: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_hash", hashlib.md5(self.source_chunk).hexdigest()
        )
        object.__setattr__(
            self, "target_hash", hashlib.md5(self.target_chunk).hexdigest()
        )
        object.__setattr__(
            self, "traceback", "".join(traceback.format_stack()).strip()
        )

    def to_dict(self) -> dict:
        dict = dataclasses.asdict(self)
        del dict["source_chunk"]
        del dict["target_chunk"]
        return dict

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
