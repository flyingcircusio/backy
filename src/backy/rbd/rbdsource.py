import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import IO, Any

from structlog.stdlib import BoundLogger

import backy

from ..ext_deps import BACKY_EXTRACT
from ..repository import Repository
from ..revision import Revision, Trust
from ..source import Source
from ..utils import CHUNK_SIZE, copy, posix_fadvise
from .chunked import ChunkedFileBackend
from .chunked.chunk import BackendException
from .quarantine import QuarantineStore
from .sources import BackySourceFactory, select_source

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
    """A backup of a VM.

    Provides access to methods to

    - backup, restore, and list revisions

    """

    source: BackySourceFactory
    quarantine: QuarantineStore

    def __init__(self, config: dict[str, Any], log: BoundLogger):

        # Initialize our source
        try:
            source_factory = select_source(self.config["source"]["type"])
        except IndexError:
            self.log.error(
                "source-type-unavailable",
                _fmt_msg="No source type named `{type}` exists.",
                type=self.config["source"]["type"],
            )
            raise
        self.source = source_factory(self.config["source"], self, self.log)

        assert self.config["source"].get("backend", "chunked") == "chunked"

        self.quarantine = QuarantineStore(self.path, self.log)

    @property
    def subcommand(self) -> str:
        return "backy-rbd"

    @property
    def problem_reports(self):
        return [f"{len(self.quarantine.report_ids)} quarantined blocks"]

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
                backend = ChunkedFileBackend(revision, self.log)
                source.backup(backend)
                verified = source.verify(backend)
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
                backend = ChunkedFileBackend(revision, self.log)
                backend.verify()
                break
        return verified

    @Repository.locked(target=".purge", mode="shared")
    def verify(self, revision: Revision) -> None:
        ChunkedFileBackend(revision, self.log).verify()

    @Repository.locked(target=".purge", mode="exclusive")
    def gc(self) -> None:
        ChunkedFileBackend(self.repository.local_history[-1], self.log).purge()
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
