from pathlib import Path
from typing import Iterable, Set

from structlog.stdlib import BoundLogger

from backy.backends.chunked.chunk import Hash

# A chunkstore, is responsible for all revisions for a single backup, for now.
# We can start having statistics later how much reuse between images is
# actually there and maybe extend this to multiple images. This then requires a
# lot more coordination about the users of the pool.


def rreplace(str, old, new):
    return new.join(str.rsplit(old, 1))


class Store(object):
    # Signal that we should always override chunks that we want to write.
    # This can be used in the face of suspected inconsistencies while still
    # wanting to perform new backups.
    force_writes = False

    path: Path
    seen_forced: set[Hash]
    known: set[Hash]
    log: BoundLogger

    def __init__(self, path: Path, log: BoundLogger):
        self.path = path
        self.seen_forced = set()
        self.log = log.bind(subsystem="chunked-store")
        for x in range(256):
            subdir = self.path / f"{x:02x}"
            subdir.mkdir(parents=True, exist_ok=True)
        if not self.path.joinpath("store").exists():
            self.convert_to_v2()

        self.known = set(self.ls())
        self.log.debug("init", known_chunks=len(self.known))

    def convert_to_v2(self) -> None:
        self.log.info("to-v2")
        for path in self.path.glob("*/*/*.chunk.lzo"):
            new = path.relative_to(self.path)
            dir, _, c = new.parts
            new = self.path / dir / c
            path.rename(new)
        for path in self.path.glob("*/??"):
            if path.is_dir():
                path.rmdir()
        with self.path.joinpath("store").open("wb") as f:
            f.write(b"v2")
        self.log.info("to-v2-finished")

    def ls(self) -> Iterable[Hash]:
        # XXX this is fucking expensive
        for file in self.path.glob("*/*.chunk.lzo"):
            yield file.name.removesuffix(".chunk.lzo")

    def purge(self, used_chunks: Set[Hash]) -> None:
        # This assumes exclusive lock on the store. This is guaranteed by
        # backy's main locking.
        to_delete = self.known - used_chunks
        self.log.info("purge", purging=len(to_delete))
        for file_hash in sorted(to_delete):
            self.chunk_path(file_hash).unlink(missing_ok=True)
        self.known -= to_delete

    def chunk_path(self, hash: Hash) -> Path:
        dir1 = hash[:2]
        extension = ".chunk.lzo"
        return self.path.joinpath(dir1).joinpath(hash).with_suffix(extension)
