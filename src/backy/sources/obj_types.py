import datetime
import os
import tempfile
from abc import ABC, abstractmethod
from asyncio import Future
from contextlib import contextmanager
from pathlib import Path
from typing import IO, TYPE_CHECKING, AsyncIterable, Iterator

import yaml
from mypy_boto3_s3.type_defs import ObjectTypeDef

from backy.utils import posix_fadvise

if TYPE_CHECKING:
    from backy.backends.s3 import Store


class TemporaryS3Obj:
    path: Path
    mode: str

    @property
    def meta_path(self) -> Path:
        return self.path.with_suffix(".meta")

    @property
    def writeable(self) -> bool:
        return "w" in self.mode or "+" in self.mode

    def __init__(self, path: Path, mode: str = "rb"):
        assert "b" in mode
        self.path = path
        self.mode = mode

    @classmethod
    def create_incoming(cls, dir: Path) -> "TemporaryS3Obj":
        fd, tmpfile_name = tempfile.mkstemp(dir=dir)
        os.close(fd)
        return cls(dir / tmpfile_name, "wb")

    def set_metadata(self, meta: dict) -> None:
        assert self.writeable
        with self.meta_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump({"Metadata": meta}, f)
            f.flush()
            os.fsync(f)

    def get_metadata(self) -> dict:
        with self.meta_path.open(encoding="utf-8") as f:
            return yaml.safe_load(f)["Metadata"]

    @contextmanager
    def open(self) -> Iterator[IO]:
        with self.path.open(self.mode) as f:
            posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)  # type: ignore
            yield f
            f.flush()
            os.fsync(f)


class RemoteS3Obj:
    key: str
    lastmodified: datetime.datetime
    etag: str

    def __init__(
        self,
        key: str,
        lastmodified: datetime.datetime,
        etag: str,
    ):
        self.key = key
        self.lastmodified = lastmodified
        self.etag = etag

    @classmethod
    def from_api(cls, obj: ObjectTypeDef) -> "RemoteS3Obj":
        return cls(obj["Key"], obj["LastModified"], obj["ETag"])


class S3Obj(TemporaryS3Obj, RemoteS3Obj):
    id: int

    def __init__(
        self,
        store: "Store",
        id: int,
        mode: str,
        key: str,
        lastmodified: datetime.datetime,
        etag: str,
    ):
        self.id = id
        TemporaryS3Obj.__init__(self, store.object_path(id), mode)
        RemoteS3Obj.__init__(self, key, lastmodified, etag)

    @classmethod
    def from_db_row(cls, store: "Store", row, mode="rb") -> "S3Obj":
        return cls(
            store, row["id"], mode, row["key"], row["lastmodified"], row["etag"]
        )


class ObjectRestoreTarget(ABC):
    async def __aenter__(self) -> "ObjectRestoreTarget":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    @abstractmethod
    async def submit_delete_obj(self, key: str) -> Future[None]:
        ...

    @abstractmethod
    async def submit_upload_obj(self, obj: S3Obj) -> Future[None]:
        ...

    @abstractmethod
    def list_obj(self, glob: str = "") -> AsyncIterable[RemoteS3Obj]:
        ...
