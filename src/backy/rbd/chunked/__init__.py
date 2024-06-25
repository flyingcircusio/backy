from pathlib import Path
from typing import Optional, Set

from structlog.stdlib import BoundLogger

from backy.revision import Revision, Trust
from backy.utils import END, report_status

from .chunk import Chunk, Hash
from .file import File
from .store import Store


class ChunkedFileBackend:
    # multiple Backends may share the same store
    STORES: dict[Path, Store] = dict()

    def __init__(self, revision: Revision, log: BoundLogger):
        self.backup = revision.backup
        self.revision = revision
        path = self.backup.path / "chunks"
        if path not in self.STORES:
            self.STORES[path] = Store(self.backup.path / "chunks", log)
        self.store = self.STORES[path]
        self.log = log.bind(subsystem="chunked")

    def open(self, mode: str = "rb", parent: Optional[Revision] = None) -> File:  # type: ignore[override]
        if "w" in mode or "+" in mode:
            if parent and not self.revision.filename.exists():
                with self.revision.filename.open(
                    "wb"
                ) as new, parent.filename.open("rb") as old:
                    # This is ok, this is just metadata, not the actual data.
                    new.write(old.read())
        overlay = False
        if mode == "o":
            mode = "rw"
            overlay = True
        file = File(self.revision.filename, self.store, mode, overlay)

        if file.writable() and self.backup.contains_distrusted:
            # "Force write"-mode if any revision is distrusted.
            self.log.warn("forcing-full")
            self.store.force_writes = True

        return file

    def purge(self) -> None:
        self.log.debug("purge")
        used_chunks: Set[Hash] = set()
        for revision in self.backup.local_history:
            if revision.backend_type != "chunked":
                continue
            used_chunks.update(
                type(self)(revision, self.log).open()._mapping.values()
            )
        self.store.purge(used_chunks)

    @report_status
    def verify(self):
        log = self.log.bind(revision_uuid=self.revision.uuid)
        log.info("verify-start")
        verified_chunks: Set[Hash] = set()

        # Load verified chunks to avoid duplicate work
        for revision in self.backup.get_history(clean=True, local=True):
            if revision.trust != Trust.VERIFIED:
                continue
            verified_chunks.update(
                type(self)(revision, self.log).open()._mapping.values()
            )

        log.debug("verify-loaded-chunks", verified_chunks=len(verified_chunks))

        errors = False
        # Go through all chunks and check them. Delete problematic ones.
        f = self.open()
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
                if self.store.chunk_path(candidate).exists():
                    try:
                        self.store.chunk_path(candidate).unlink()
                    except Exception:
                        log.exception("verify-remove-error", chunk=candidate)
                # This is an optimisation: we can skip this revision, purge it
                # and then keep verifying other chunks. This avoids checking
                # things unnecessarily in duplicate.
                # And we only mark it as verified if we never saw any problems.
                break

        yield

        if errors:
            # Found any issues? Delete this revision as we can't trust it.
            self.revision.remove()
        else:
            # No problems found - mark as verified.
            self.revision.verify()
            self.revision.write_info()

        yield

        # Purge to ensure that we don't leave unused, potentially untrusted
        # stuff around, especially if this was the last revision.
        self.purge()

        yield END
        yield None
