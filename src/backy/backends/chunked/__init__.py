import os.path

from structlog.stdlib import BoundLogger

from backy.revision import Revision, Trust
from backy.utils import END, report_status

from .. import BackyBackend
from .chunk import Chunk
from .file import File
from .store import Store


class ChunkedFileBackend(BackyBackend):
    # Normally a new revision will be made by copying the last revision's file.
    # We need to be able to not do this in case of converting from a different
    # format.
    clone_parent = True

    # multiple Backends may share the same store
    STORES: dict[str, Store] = dict()

    def __init__(self, revision: Revision, log: BoundLogger):
        self.backup = revision.backup
        self.revision = revision
        path = self.revision.backup.path + "/chunks"
        if path not in self.STORES:
            self.STORES[path] = Store(
                self.revision.backup.path + "/chunks", log
            )
        self.store = self.STORES[path]
        self.log = log.bind(subsystem="chunked")

    def open(self, mode="rb"):
        if "w" in mode or "+" in mode and self.clone_parent:
            parent = self.revision.get_parent()
            if parent and not os.path.exists(self.revision.filename):
                with open(self.revision.filename, "wb") as new, open(
                    parent.filename, "rb"
                ) as old:
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

    def purge(self):
        self.log.debug("purge")
        self.store.users = []
        for revision in self.backup.history:
            try:
                self.store.users.append(
                    self.backup.backend_factory(revision, self.log).open()
                )
            except ValueError:
                # Invalid format, like purging non-chunked with chunked backend
                pass
        self.store.purge()

    @report_status
    def verify(self):
        log = self.log.bind(revision_uuid=self.revision.uuid)
        log.info("verify-start")
        verified_chunks = set()

        # Load verified chunks to avoid duplicate work
        for revision in self.backup.clean_history:
            if revision.trust != Trust.VERIFIED:
                continue
            f = self.backup.backend_factory(revision, log).open()
            verified_chunks.update(f._mapping.values())

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
                c = Chunk(f, 0, self.store, candidate)
                c._read_existing()
            except Exception:
                log.exception("verify-error", chunk=candidate)
                errors = True
                if os.path.exists(self.store.chunk_path(candidate)):
                    try:
                        os.unlink(self.store.chunk_path(candidate))
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

    def scrub(self, backup, type):
        if type == "light":
            return self.scrub_light(backup)
        elif type == "deep":
            return self.scrub_deep(backup)
        else:
            raise RuntimeError("Invalid scrubbing type {}".format(type))

    def scrub_light(self, backup):
        errors = 0
        self.log.info("scrub-light")
        for revision in backup.history:
            self.log.info("scrub-light-rev", revision_uuid=revision.uuid)
            backend = backup.backend_factory(revision, self.log).open()
            for hash in backend._mapping.values():
                if os.path.exists(backend.store.chunk_path(hash)):
                    continue
                self.log.error(
                    "scrub-light-missing-chunk",
                    hash=hash,
                    revision_uuid=revision.uuid,
                )
                errors += 1
        return errors

    def scrub_deep(self, backup):
        errors = self.scrub_light(backup)
        self.log.info("scrub-deep")
        errors += self.store.validate_chunks()
        return errors
