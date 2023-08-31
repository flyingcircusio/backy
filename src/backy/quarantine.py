import datetime
import glob
import hashlib
import os
import traceback
from os import path as p
from typing import List

import shortuuid
import yaml
from structlog.stdlib import BoundLogger
from yaml import SafeDumper

import backy
from backy.utils import SafeFile


class QuarantineReport:
    uuid: str
    source_chunk: bytes
    source_hash: str
    target_chunk: bytes
    target_hash: str
    offset: int
    timestamp: datetime.datetime
    traceback: str

    def __init__(self, source_chunk, target_chunk, offset):
        self.uuid = shortuuid.uuid()
        self.source_chunk = source_chunk
        self.source_hash = hashlib.md5(source_chunk).hexdigest()
        self.target_chunk = target_chunk
        self.target_hash = hashlib.md5(target_chunk).hexdigest()
        self.offset = offset
        self.timestamp = backy.utils.now()
        self.traceback = "".join(traceback.format_stack()).strip()

    def to_dict(self) -> dict:
        return {
            "uuid": self.uuid,
            "source_hash": self.source_hash,
            "target_hash": self.target_hash,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "traceback": self.traceback,
        }


class QuarantineStore:
    path: str
    chunks_path: str
    report_ids: List[str]
    log: BoundLogger

    def __init__(self, backup_path, log):
        self.path = p.join(backup_path, "quarantine")
        if not p.exists(self.path):
            os.mkdir(self.path)
        self.chunks_path = p.join(self.path, "chunks")
        if not p.exists(self.chunks_path):
            os.mkdir(self.chunks_path)
        self.log = log.bind(subsystem="quarantine")
        self.scan()

    def add_report(self, report: QuarantineReport):
        self.log.info("add-report", uuid=report.uuid)
        self._store_chunk(report.source_chunk, report.source_hash)
        self._store_chunk(report.target_chunk, report.target_hash)
        self._store_report(report)

        self.report_ids.append(report.uuid)

    def _store_report(self, report: QuarantineReport):
        self.log.debug("store-report", uuid=report.uuid)
        path = p.join(self.path, f"{report.uuid}.report")
        if os.path.exists(path):
            self.log.debug("store-report-exists", uuid=report.uuid)
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
            yaml.dump(report.to_dict(), f, sort_keys=False, Dumper=CustomDumper)

    def _store_chunk(self, chunk: bytes, hash: str):
        self.log.debug("store-chunk", hash=hash)
        path = p.join(self.chunks_path, hash)
        if os.path.exists(path):
            self.log.debug("store-chunk-exists", hash=hash)
            return
        with SafeFile(path) as f:
            f.open_new("wb")
            f.write(chunk)

    def scan(self):
        self.report_ids = [
            p.basename(g) for g in glob.glob(p.join(self.path, "*.report"))
        ]
        self.log.debug("scan", entries=len(self.report_ids))
