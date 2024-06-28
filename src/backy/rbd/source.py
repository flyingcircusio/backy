import json
import os
import subprocess
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import IO, Any, Optional, Set

import consulate
from structlog.stdlib import BoundLogger

import backy
import backy.utils
from backy.ext_deps import BACKY_EXTRACT
from backy.repository import Repository
from backy.revision import Revision, Trust
from backy.source import Source
from backy.utils import (
    CHUNK_SIZE,
    END,
    TimeOut,
    TimeOutError,
    copy,
    posix_fadvise,
    report_status,
)

from .chunked import BackendException, Chunk, File, Hash, Store
from .quarantine import QuarantineReport, QuarantineStore
from .rbd import RBDClient


class RestoreBackend(Enum):
    AUTO = "auto"
    PYTHON = "python"
    RUST = "rust"

    def __str__(self):
        return self.value


class RestoreArgs:
    target: str
    backend: RestoreBackend


class RbdSource(Source[RestoreArgs]):
    type_ = "rbd"
    subcommand = "backy-rbd"

    source: "FlyingCircusRootDisk"
    store: Store
    quarantine: QuarantineStore
    log: BoundLogger

    def __init__(self, rbdsource: "FlyingCircusRootDisk", log: BoundLogger):
        self.source = rbdsource
        self.log = log.bind(subsystem="rbdsource")

    @classmethod
    def from_config(cls, config: dict[str, Any], log: BoundLogger) -> "Source":
        assert config.get("backend", "chunked") == "chunked"
        return cls(FlyingCircusRootDisk(config, log), log)

    def bind(self, repository: "Repository") -> None:
        super().bind(repository)
        # TODO: move quarantine to repo
        self.quarantine = QuarantineStore(repository.path, self.log)
        self.store = Store(repository.path / "chunks", self.log)

    def _path_for_revision(self, revision: Revision) -> Path:
        return self.repository.path / revision.uuid

    def open(
        self, revision: Revision, parent: Optional[Revision] = None
    ) -> File:
        if parent and not self._path_for_revision(revision).exists():
            with self._path_for_revision(revision).open(
                "wb"
            ) as new, self._path_for_revision(parent).open("rb") as old:
                # This is ok, this is just metadata, not the actual data.
                new.write(old.read())
        file = File(self._path_for_revision(revision), self.store)

        if file.writable() and self.repository.contains_distrusted:
            # "Force write"-mode if any revision is distrusted.
            self.log.warn("forcing-full")
            self.store.force_writes = True

        return file

    #################
    # Making backups

    @Repository.locked(target=".backup", mode="exclusive")
    @Repository.locked(target=".purge", mode="shared")
    def backup(self, revision: Revision) -> bool:
        self.repository.path.joinpath("last").unlink(missing_ok=True)
        self.repository.path.joinpath("last.rev").unlink(missing_ok=True)

        start = time.time()

        if not self.source.ready():
            raise RuntimeError(
                "Source is not ready (does it exist? can you access it?)"
            )

        with self.source(revision) as source:
            try:
                parent_rev = source.get_parent()
                file = self.open(revision, parent_rev)
                if parent_rev:
                    source.full(file)
                else:
                    source.diff(file, parent_rev)
                file = self.open(revision)
                verified = source.verify(file, self.quarantine)
            except BackendException:
                self.log.exception("backend-error-distrust-all")
                verified = False
                self.repository.distrust("local", skip_lock=True)
            if not verified:
                self.log.error(
                    "verification-failed",
                    revision_uuid=revision.uuid,
                )
                revision.remove()
            else:
                self.log.info("verification-ok", revision_uuid=revision.uuid)
                revision.stats["duration"] = time.time() - start
                revision.write_info()
                revision.readonly()
            # Switched from a fine-grained syncing mechanism to "everything
            # once" when we're done. This is as safe but much faster.
            os.sync()

        # If there are distrusted revisions, then perform at least one
        # verification after a backup - for good measure and to keep things
        # moving along automatically. This could also be moved into the
        # scheduler.
        self.repository.scan()
        # TODO: move this to cli/daemon?
        for revision in reversed(
            self.repository.get_history(clean=True, local=True)
        ):
            if revision.trust == Trust.DISTRUSTED:
                self.log.warning("inconsistent")
                self.verify(revision, skip_lock=True)
                break
        return verified

    @Repository.locked(target=".purge", mode="shared")
    @report_status
    def verify(self, revision: Revision):
        log = self.log.bind(revision_uuid=revision.uuid)
        log.info("verify-start")
        verified_chunks: Set[Hash] = set()

        # Load verified chunks to avoid duplicate work
        for verified_revision in self.repository.get_history(
            clean=True, local=True
        ):
            if verified_revision.trust != Trust.VERIFIED:
                continue
            verified_chunks.update(
                self.open(verified_revision)._mapping.values()
            )

        log.debug("verify-loaded-chunks", verified_chunks=len(verified_chunks))

        errors = False
        # Go through all chunks and check them. Delete problematic ones.
        f = self.open(revision)
        hashes = set(f._mapping.values()) - verified_chunks
        yield len(hashes) + 2
        for candidate in hashes:
            yield
            if candidate in verified_chunks:
                continue
            try:
                c = Chunk(self.store, candidate)
                c._read_existing()
            except Exception:
                log.exception("verify-error", chunk=candidate)
                errors = True
                try:
                    self.store.chunk_path(candidate).unlink(missing_ok=True)
                except Exception:
                    log.exception("verify-remove-error", chunk=candidate)
                # This is an optimisation: we can skip this revision, purge it
                # and then keep verifying other chunks. This avoids checking
                # things unnecessarily in duplicate.
                # And we only mark it as verified if we never saw any problems.
                break

        yield

        # TODO: move this to cli/daemon?
        if errors:
            # Found any issues? Delete this revision as we can't trust it.
            revision.remove()
        else:
            # No problems found - mark as verified.
            revision.verify()
            revision.write_info()

        yield

        # Purge to ensure that we don't leave unused, potentially untrusted
        # stuff around, especially if this was the last revision.
        self.gc(skip_lock=True)

        yield END
        yield None

    @Repository.locked(target=".purge", mode="exclusive")
    def gc(self) -> None:
        self.log.debug("purge")
        used_chunks: Set[Hash] = set()
        for revision in self.repository.local_history:
            used_chunks.update(self.open(revision)._mapping.values())
        self.store.purge(used_chunks)
        # TODO: move this to cli/daemon?
        self.repository.clear_purge_pending()

    #################
    # Restoring

    # This needs no locking as it's only a wrapper for restore_file and
    # restore_stdout and locking isn't re-entrant.
    def restore(self, revision: Revision, args: RestoreArgs) -> None:
        s = self.open(revision)
        if args.backend == RestoreBackend.AUTO:
            if self.backy_extract_supported(s):
                restore_backend = RestoreBackend.RUST
            else:
                restore_backend = RestoreBackend.PYTHON
            self.log.info("restore-backend", backend=restore_backend.value)
        if restore_backend == RestoreBackend.PYTHON:
            with s as source:
                if args.target != "-":
                    self.restore_file(source, args.target)
                else:
                    self.restore_stdout(source)
        elif restore_backend == RestoreBackend.RUST:
            self.restore_backy_extract(revision, args.target)

    def backy_extract_supported(self, file: "backy.rbd.chunked.File") -> bool:
        log = self.log.bind(subsystem="backy-extract")
        if file.size % CHUNK_SIZE != 0:
            log.debug("not-chunk-aligned")
            return False
        try:
            version = subprocess.check_output(
                [BACKY_EXTRACT, "--version"],
                encoding="utf-8",
                errors="replace",
            )
            if not version.startswith("backy-extract"):
                log.debug("unknown-version")
                return False
        except Exception:
            log.debug("unavailable")
            return False
        return True

    # backy-extract acquires lock
    def restore_backy_extract(self, rev: Revision, target: str) -> None:
        log = self.log.bind(subsystem="backy-extract")
        cmd = [BACKY_EXTRACT, str(self.repository.path / rev.uuid), target]
        log.debug("started", cmd=cmd)
        proc = subprocess.Popen(cmd)
        return_code = proc.wait()
        log.info(
            "finished",
            return_code=return_code,
            subprocess_pid=proc.pid,
        )
        if return_code:
            raise RuntimeError(
                f"backy-extract failed with return code {return_code}. "
                "Maybe try `--backend python`?"
            )

    @Repository.locked(target=".purge", mode="shared")
    def restore_file(self, source: IO, target_name: str) -> None:
        """Bulk-copy from open revision `source` to target file."""
        self.log.debug("restore-file", source=source.name, target=target_name)
        open(target_name, "ab").close()  # touch into existence
        with open(target_name, "r+b", buffering=CHUNK_SIZE) as target:
            try:
                posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)  # type: ignore
            except Exception:
                pass
            copy(source, target)

    @Repository.locked(target=".purge", mode="shared")
    def restore_stdout(self, source: IO) -> None:
        """Emit restore data to stdout (for pipe processing)."""
        self.log.debug("restore-stdout", source=source.name)
        try:
            posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)  # type: ignore
        except Exception:
            pass
        with os.fdopen(os.dup(1), "wb") as target:
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)


class CephRBD:
    """The Ceph RBD source.

    Manages snapshots corresponding to revisions and provides a verification
    that tries to balance reliability and performance.
    """

    pool: str
    image: str
    always_full: bool
    log: BoundLogger
    rbd: RBDClient
    revision: Revision
    snapshot_timeout = 90

    def __init__(self, config: dict, log: BoundLogger):
        self.pool = config["pool"]
        self.image = config["image"]
        self.always_full = config.get("full-always", False)
        self.log = log.bind(subsystem="ceph")
        self.rbd = RBDClient(self.log)
        self.vm = config["vm"]
        self.consul_acl_token = config.get("consul_acl_token")

    def ready(self) -> bool:
        """Check whether the source can be backed up.

        For RBD sources this means the volume exists and is accessible.

        """
        try:
            if self.rbd.exists(self._image_name):
                return True
        except Exception:
            self.log.exception("not-ready")
        return False

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        snapname = "backy-{}".format(self.revision.uuid)
        self.create_snapshot(snapname)
        return self

    def create_snapshot(self, snapname: str) -> None:
        """An overridable method to allow different ways of creating the
        snapshot.
        """
        self.rbd.snap_create(self._image_name + "@" + snapname)

    @property
    def _image_name(self) -> str:
        return "{}/{}".format(self.pool, self.image)

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self._delete_old_snapshots()

    def get_parent(self) -> Optional[Revision]:
        if self.always_full:
            self.log.info("backup-always-full")
            return None
        revision = self.revision
        while True:
            parent = revision.get_parent()
            if not parent:
                self.log.info("backup-no-valid-parent")
                return None
            if not self.rbd.exists(self._image_name + "@backy-" + parent.uuid):
                self.log.info(
                    "ignoring-rev-without-snapshot",
                    revision_uuid=parent.uuid,
                )
                revision = parent
                continue
            # Ok, it's trusted and we have a snapshot. Let's do a diff.
            return parent

    def diff(self, target: "RbdSource", parent: Revision) -> None:
        self.log.info("diff")
        snap_from = "backy-" + parent.uuid
        snap_to = "backy-" + self.revision.uuid
        s = self.rbd.export_diff(self._image_name + "@" + snap_to, snap_from)
        with s as source, target.open(self.revision, parent) as target_:
            bytes = source.integrate(target_, snap_from, snap_to)
        self.log.info("diff-integration-finished")

        self.revision.stats["bytes_written"] = bytes

        # TMP Gather statistics to see where to optimize
        from backy.rbd.chunked.chunk import chunk_stats

        self.revision.stats["chunk_stats"] = chunk_stats

    def full(self, target: File) -> None:
        self.log.info("full")
        s = self.rbd.export(
            "{}/{}@backy-{}".format(self.pool, self.image, self.revision.uuid)
        )
        copied = 0
        with s as source, target as target_:
            while True:
                buf = source.read(4 * backy.utils.MiB)
                if not buf:
                    break
                target_.write(buf)
                copied += len(buf)
        self.revision.stats["bytes_written"] = copied

        # TMP Gather statistics to see if we actually are aligned.
        from backy.rbd.chunked.chunk import chunk_stats

        self.revision.stats["chunk_stats"] = chunk_stats

    def verify(self, target: File, quarantine: QuarantineStore) -> bool:
        s = self.rbd.image_reader(
            "{}/{}@backy-{}".format(self.pool, self.image, self.revision.uuid)
        )
        self.revision.stats["ceph-verification"] = "partial"

        with s as source, target as target_:
            self.log.info("verify")
            return backy.utils.files_are_roughly_equal(
                source,
                target_,
                report=lambda s, t, o: quarantine.add_report(
                    QuarantineReport(s, t, o)
                ),
            )

    def _delete_old_snapshots(self) -> None:
        # Clean up all snapshots except the one for the most recent valid
        # revision.
        # Previously we used to remove all snapshots but the one for this
        # revision - which is wrong: broken new revisions would always cause
        # full backups instead of new deltas based on the most recent valid
        # one.
        # XXX this will break if multiple servers are active
        if not self.always_full and self.revision.repository.local_history:
            keep_snapshot_revision = self.revision.repository.local_history[
                -1
            ].uuid
        else:
            keep_snapshot_revision = None
        for snapshot in self.rbd.snap_ls(self._image_name):
            if not snapshot["name"].startswith("backy-"):
                # Do not touch non-backy snapshots
                continue
            uuid = snapshot["name"].replace("backy-", "")
            if uuid != keep_snapshot_revision:
                time.sleep(3)  # avoid race condition while unmapping
                self.log.info(
                    "delete-old-snapshot", snapshot_name=snapshot["name"]
                )
                try:
                    self.rbd.snap_rm(self._image_name + "@" + snapshot["name"])
                except Exception:
                    self.log.exception(
                        "delete-old-snapshot-failed",
                        snapshot_name=snapshot["name"],
                    )

    def create_snapshot(self, name: str) -> None:
        consul = consulate.Consul(token=self.consul_acl_token)
        snapshot_key = "snapshot/{}".format(str(uuid.uuid4()))
        self.log.info(
            "creating-snapshot",
            snapshot_name=name,
            snapshot_key=snapshot_key,
        )

        consul.kv[snapshot_key] = {"vm": self.vm, "snapshot": name}

        time.sleep(3)
        try:
            timeout = TimeOut(
                self.snapshot_timeout, interval=2, raise_on_timeout=True
            )
            while timeout.tick():
                for snapshot in self.rbd.snap_ls(self._image_name):
                    if snapshot["name"] == name:
                        return
        except TimeOutError:
            # The VM might have been shut down. Try doing a regular Ceph
            # snapshot locally.
            super(FlyingCircusRootDisk, self).create_snapshot(name)
        except KeyboardInterrupt:
            raise
        finally:
            # In case the snapshot still gets created: the general snapshot
            # deletion code in ceph/source will clean up unused backy snapshots
            # anyway. However, we need to work a little harder to delete old
            # snapshot requests, otherwise we've sometimes seen those not
            # getting deleted and then re-created all the time.
            for key in list(consul.kv.find("snapshot/")):
                try:
                    s = consul.kv[key]
                except KeyError:
                    continue
                try:
                    s = json.loads(s)
                except json.decoder.JSONDecodeError:
                    # Clean up garbage.
                    self.log.warning(
                        "create-snapshot-removing-garbage-request",
                        snapshot_key=key,
                    )
                    del consul.kv[key]
                if s["vm"] != self.vm:
                    continue
                # The knowledge about the `backy-` prefix  isn't properly
                # encapsulated here.
                if s["snapshot"].startswith("backy-"):
                    self.log.info(
                        "create-snapshot-removing-request",
                        vm=s["vm"],
                        snapshot_name=s["snapshot"],
                        snapshot_key=key,
                    )
                    del consul.kv[key]
