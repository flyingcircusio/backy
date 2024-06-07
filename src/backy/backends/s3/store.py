import datetime
import sqlite3
from pathlib import Path
from sqlite3 import PARSE_DECLTYPES, Cursor
from typing import Iterable, Optional, Set

from structlog.stdlib import BoundLogger

from backy.revision import Revision
from backy.sources.obj_types import RemoteS3Obj, S3Obj, TemporaryS3Obj

STORE_DEPTH = 3

sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "timestamp", lambda s: datetime.datetime.fromisoformat(s.decode())
)


class Store(object):
    path: Path
    log: BoundLogger
    db: sqlite3.Connection
    incoming_path: Path

    def __init__(self, path: Path, log: BoundLogger):
        self.path = path
        self.path.mkdir(exist_ok=True)
        self.incoming_path = self.path / "incoming"
        self.incoming_path.mkdir(exist_ok=True)
        self.log = log.bind(subsystem="s3-store")
        self.db = sqlite3.connect(
            path / "index.db", detect_types=PARSE_DECLTYPES
        )
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS object(id INTEGER NOT NULL PRIMARY KEY, key VARCHAR NOT NULL, lastmodified timestamp NOT NULL, etag VARCHAR NOT NULL)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS obj_key_idx ON object(key)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS revision(id INTEGER NOT NULL PRIMARY KEY, name VARCHAR NOT NULL UNIQUE)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS rev_obj(revision INTEGER NOT NULL REFERENCES revision(id) ON DELETE CASCADE, object INTEGER NOT NULL REFERENCES object(id) ON DELETE CASCADE, PRIMARY KEY (revision, object))"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS rev_obj_obj_idx ON rev_obj(object)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS rev_obj_rev_idx ON rev_obj(revision)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS rev_obj(revision INTEGER NOT NULL REFERENCES revision(id) ON DELETE CASCADE, object INTEGER NOT NULL REFERENCES object(id) ON DELETE CASCADE, PRIMARY KEY (revision, object))"
            )

    def get_rev_id(self, revision: Revision) -> Optional[int]:
        o = self.db.execute(
            "SELECT id FROM revision WHERE name = ?", (revision.uuid,)
        ).fetchone()
        if o:
            return o["id"]
        return None

    def get_or_create_rev(self, revision: Revision) -> int:
        self.db.execute(
            "INSERT OR IGNORE INTO revision(name) VALUES(?)",
            (revision.uuid,),
        )
        id = self.get_rev_id(revision)
        assert id is not None
        return id

    def get_object(self, key: str, rev: int) -> Optional["S3Obj"]:
        obj = self.db.execute(
            "SELECT id, key, lastmodified, etag FROM object INNER JOIN rev_obj ON rev_obj.object = object.id WHERE object.key = ? AND rev_obj.revision = ?",
            (key, rev),
        ).fetchone()
        if obj:
            return S3Obj.from_db_row(self, obj)
        return None

    def add_obj_rev_relation(self, obj_id: int, rev_id: int) -> None:
        self.db.execute(
            "INSERT INTO rev_obj(revision, object) VALUES(?, ?)",
            (rev_id, obj_id),
        )

    def add_object(self, obj: RemoteS3Obj) -> "S3Obj":
        res = self.db.execute(
            "INSERT INTO object(key, lastmodified, etag) VALUES(?, ?, ?) RETURNING id, key, lastmodified, etag",
            (obj.key, obj.lastmodified, obj.etag),
        ).fetchone()
        return S3Obj.from_db_row(self, res, "rb")

    def list_revisions(self) -> Cursor:
        return self.db.execute("SELECT id, name FROM revision")

    def list_obj(self, rev: int) -> Iterable["S3Obj"]:
        cur = self.db.execute(
            "SELECT id, key, lastmodified, etag FROM object INNER JOIN rev_obj ON rev_obj.object = object.id WHERE rev_obj.revision = ?",
            (rev,),
        )
        for row in cur:
            yield S3Obj.from_db_row(self, row)

    def ls(self) -> Iterable[int]:
        # XXX this is fucking expensive
        for file in self.path.rglob("*.object"):
            yield int(file.name.removesuffix(".object"))

    def purge(self, existing_revs: Set[str]) -> None:
        with self.db:
            for id, name in self.list_revisions():
                if name not in existing_revs:
                    self.log.info("purge-rev", rev=name)
                    self.db.execute("DELETE FROM revision WHERE id = ?", (id,))
        with self.db:
            # self.db.execute("DELETE FROM object LEFT OUTER JOIN rev_obj ON object.id = rev_obj.object WHERE rev_obj.revision IS NULL")
            cursor = self.db.execute(
                "DELETE FROM object WHERE NOT EXISTS (SELECT 1 from rev_obj WHERE rev_obj.object = object.id) RETURNING object.id"
            )
            # it is ok to remove the files before commiting the transaction
            # because the row entries will be removed by the next purge call
            # and won't be used by a new revision because the parent no longer exists
            num = 0
            for row in cursor:
                num += 1
                self.object_path(row["id"]).unlink(missing_ok=True)
            self.log.info("purge-obj", num=num)

        for id in self.ls():
            if not self.db.execute(
                "SELECT 1 FROM object WHERE id = ?", (id,)
            ).fetchone():
                self.object_path(id).unlink(missing_ok=True)

    def verify(self, revision: Revision):
        rev_id = self.get_rev_id(revision)
        assert rev_id
        for obj in self.list_obj(rev_id):
            pass
            # TODO check integrity

    def object_path(self, id: int) -> Path:
        path = self.path
        for i in reversed(range(1, STORE_DEPTH + 1)):
            path = path.joinpath(f"{(id>>(i*16)) & 0xffff:04x}")
        return path.joinpath(f"{id:016x}").with_suffix(".object")

    def create_incoming_obj(self) -> "TemporaryS3Obj":
        return TemporaryS3Obj.create_incoming(self.incoming_path)
