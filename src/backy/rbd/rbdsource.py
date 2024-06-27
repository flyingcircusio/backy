import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import IO, Any, Optional, Set

from structlog.stdlib import BoundLogger

import backy

from ..ext_deps import BACKY_EXTRACT
from ..repository import Repository
from ..revision import Revision, Trust
from ..source import Source
from ..utils import CHUNK_SIZE, END, copy, posix_fadvise, report_status
from .chunked.chunk import BackendException, Chunk, Hash
from .chunked.file import File
from .chunked.store import Store
from .quarantine import QuarantineStore
from .sources.flyingcircus.source import FlyingCircusRootDisk

# Locking strategy:
#
# - You can only run one backup of a machine at a time, as the backup will
#   interact with this machines' list of snapshots and will get confused
#   if run in parallel.
# - You can restore while a backup is running.
# - You can only purge while nothing else is happening.
# - Trying to get a shared lock (specifically purge) will block and wait
#   whereas trying to get an exclusive lock (running backups, purging) will
#   immediately give up.
# - Locking is not re-entrant. It's forbidden and protected to call another
#   locking main function.


class RestoreBackend(Enum):
    AUTO = "auto"
    PYTHON = "python"
    RUST = "rust"

    def __str__(self):
        return self.value


class RbdSource(Source):
    type_ = "rbd"
    subcommand = "backy-rbd"

    source: FlyingCircusRootDisk
    store: Store
    quarantine: QuarantineStore
    log: BoundLogger

    def __init__(self, rbdsource: FlyingCircusRootDisk, log: BoundLogger):
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
        self.store = Store(repository.path / "chunks")

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
    def restore(
        self,
        revision: Revision,
        target: str,
        restore_backend: RestoreBackend = RestoreBackend.AUTO,
    ) -> None:
        s = ChunkedFileBackend(revision, self.log).open("rb")
        if restore_backend == RestoreBackend.AUTO:
            if self.backy_extract_supported(s):
                restore_backend = RestoreBackend.RUST
            else:
                restore_backend = RestoreBackend.PYTHON
            self.log.info("restore-backend", backend=restore_backend.value)
        if restore_backend == RestoreBackend.PYTHON:
            with s as source:
                if target != "-":
                    self.restore_file(source, target)
                else:
                    self.restore_stdout(source)
        elif restore_backend == RestoreBackend.RUST:
            self.restore_backy_extract(revision, target)

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
        except:
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
                f"backy-extract failed with return code {return_code}. Maybe try `--backend python`?"
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
