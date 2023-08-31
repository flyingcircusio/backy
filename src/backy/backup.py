import datetime
import fcntl
import glob
import os
import os.path as p
import time
from typing import IO, Optional, Type

import yaml
from structlog.stdlib import BoundLogger

from backy.utils import min_date

from .backends import BackendException, BackyBackend
from .backends.chunked import ChunkedFileBackend
from .backends.cowfile import COWFileBackend
from .quarantine import QuarantineStore
from .revision import Revision, Trust, filter_schedule_tags
from .schedule import Schedule
from .sources import BackySourceFactory, select_source
from .utils import CHUNK_SIZE, copy, posix_fadvise

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


def locked(target=None, mode=None):
    if mode == "shared":
        mode = fcntl.LOCK_SH
    elif mode == "exclusive":
        mode = fcntl.LOCK_EX | fcntl.LOCK_NB
    else:
        raise ValueError("Unknown lock mode '{}'".format(mode))

    def wrap(f):
        def locked_function(self, *args, **kw):
            if target in self._lock_fds:
                raise RuntimeError("Bug: Locking is not re-entrant.")
            target_path = p.join(self.path, target)
            if not os.path.exists(target_path):
                open(target_path, "wb").close()
            self._lock_fds[target] = os.open(target_path, os.O_RDONLY)
            try:
                fcntl.flock(self._lock_fds[target], mode)
            except BlockingIOError:
                self.log.warning(
                    "lock-no-exclusive",
                    _fmt_msg="Failed to get exclusive lock for '{function}'. Continuing.",
                    function=f.__name__,
                )
                return
            else:
                try:
                    return f(self, *args, **kw)
                finally:
                    fcntl.flock(self._lock_fds[target], fcntl.LOCK_UN)
            finally:
                del self._lock_fds[target]

        locked_function.__name__ = "locked({}, {})".format(f.__name__, target)
        return locked_function

    return wrap


class Backup(object):
    """A backup of a VM.

    Provides access to methods to

    - backup, restore, and list revisions

    """

    path: str
    config: dict
    schedule: Schedule
    source: BackySourceFactory
    backend_type: str
    backend_factory: Type[BackyBackend]
    history: list[Revision]
    quarantine: QuarantineStore
    log: BoundLogger

    _by_uuid: dict[str, Revision]
    _lock_fds: dict[str, IO]

    def __init__(self, path, log):
        self.log = log.bind(subsystem="backup")
        self._lock_fds = {}

        self.path = p.realpath(path)
        self.scan()

        # Load config from file
        try:
            with open(p.join(self.path, "config"), encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
        except IOError:
            self.log.error(
                "could-not-read-config",
                _fmt_msg="Could not read config file. Is --backupdir correct?",
                config_path=p.join(self.path, "config"),
            )
            raise

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
        self.source = source_factory(self.config["source"], self.log)

        # Initialize our backend
        self.backend_type = self.config["source"].get("backend", None)
        if self.backend_type is None:
            if not self.history:
                # Start fresh backups with our new default.
                self.backend_type = "chunked"
            else:
                # Choose to continue existing backups with whatever format
                # they are in.
                self.backend_type = self.history[-1].backend_type

        self.schedule = Schedule()
        self.schedule.configure(self.config["schedule"])

        if self.backend_type == "cowfile":
            self.backend_factory = COWFileBackend
        elif self.backend_type == "chunked":
            self.backend_factory = ChunkedFileBackend
        else:
            raise ValueError(
                "Unsupported backend_type '{}'".format(self.backend_type)
            )

        self.quarantine = QuarantineStore(self.path, self.log)

    def scan(self):
        self.history = []
        self._by_uuid = {}
        for f in glob.glob(p.join(self.path, "*.rev")):
            if os.path.islink(f):
                # Ignore links that are used to create readable pointers
                continue
            r = Revision.load(f, self, self.log)
            if r.uuid not in self._by_uuid:
                self._by_uuid[r.uuid] = r
                self.history.append(r)
        # The history is stored: oldest first. newest last.
        self.history.sort(key=lambda r: r.timestamp)

    @property
    def clean_history(self):
        """History without incomplete revisions."""
        return [rev for rev in self.history if "duration" in rev.stats]

    @property
    def contains_distrusted(self):
        return any((r == Trust.DISTRUSTED for r in self.clean_history))

    #################
    # Making backups

    @locked(target=".backup", mode="exclusive")
    def _clean(self):
        """Clean-up incomplete revisions."""
        for revision in self.history:
            if "duration" not in revision.stats:
                self.log.warning(
                    "clean-incomplete", revision_uuid=revision.uuid
                )
                revision.remove()

    @locked(target=".backup", mode="exclusive")
    def forget_revision(self, revision):
        r = self.find(revision)
        r.remove()

    @locked(target=".backup", mode="exclusive")
    @locked(target=".purge", mode="shared")
    def backup(self, tags, force=False):
        if not force:
            missing_tags = (
                filter_schedule_tags(tags) - self.schedule.schedule.keys()
            )
            if missing_tags:
                self.log.error(
                    "unknown-tags",
                    _fmt_msg="The following tags are missing from the schedule: {unknown_tags}\n"
                    "Check the config file, add the `manual:` prefix or disable tag validation (-f)",
                    unknown_tags=", ".join(missing_tags),
                )
                raise RuntimeError("Unknown tags")

        try:  # cleanup old symlinks
            os.unlink(p.join(self.path, "last"))
            os.unlink(p.join(self.path, "last.rev"))
        except OSError:
            pass

        start = time.time()

        if not self.source.ready():
            raise RuntimeError(
                "Source is not ready (does it exist? can you access it?)"
            )

        new_revision = Revision.create(self, tags, self.log)
        new_revision.materialize()
        self.log.info(
            "created-revision",
            revision_uuid=new_revision.uuid,
            tags=", ".join(new_revision.tags),
        )

        backend = self.backend_factory(new_revision, self.log)
        with self.source(new_revision) as source:
            try:
                source.backup(backend)
                verified = source.verify(backend)
            except BackendException:
                self.log.exception("backend-error-distrust-all")
                verified = False
                self.distrust_range()
            if not verified:
                self.log.error(
                    "verification-failed",
                    revision_uuid=new_revision.uuid,
                )
                new_revision.remove()
            else:
                self.log.info(
                    "verification-ok", revision_uuid=new_revision.uuid
                )
                new_revision.stats["duration"] = time.time() - start
                new_revision.write_info()
                new_revision.readonly()
                self.scan()
            # Switched from a fine-grained syncing mechanism to "everything
            # once" when we're done. This is as safe but much faster.
            os.sync()

        # If there are distrusted revisions, then perform at least one
        # verification after a backup - for good measure and to keep things
        # moving along automatically. This could also be moved into the
        # scheduler.
        self.scan()
        for revision in reversed(self.clean_history):
            if revision.trust == Trust.DISTRUSTED:
                self.log.warning("inconsistent")
                backend = self.backend_factory(revision, self.log)
                backend.verify()
                break

    @locked(target=".backup", mode="exclusive")
    def distrust(
        self,
        revision=None,
        from_: Optional[datetime.date] = None,
        until: Optional[datetime.date] = None,
    ):
        if revision:
            r = self.find(revision)
            r.distrust()
            r.write_info()
        else:
            self.distrust_range(from_, until)

    def distrust_range(
        self,
        from_: Optional[datetime.date] = None,
        until: Optional[datetime.date] = None,
    ):
        for r in self.clean_history:
            if from_ and r.timestamp.date() < from_:
                continue
            if until and r.timestamp.date() > until:
                continue
            r.distrust()
            r.write_info()

    @locked(target=".purge", mode="shared")
    def verify(self, revision=None):
        if revision:
            r = self.find(revision)
            backend = self.backend_factory(r, self.log)
            backend.verify()
        else:
            for r in list(self.clean_history):
                if r.trust != Trust.DISTRUSTED:
                    continue
                backend = self.backend_factory(r, self.log)
                backend.verify()

    @locked(target=".purge", mode="exclusive")
    def purge(self):
        backend = self.backend_factory(self.history[0], self.log)
        backend.purge()

    #################
    # Restoring

    # This needs no locking as it's only a wrapper for restore_file and
    # restore_stdout and locking isn't re-entrant.
    def restore(self, revision, target):
        r = self.find(revision)
        backend = self.backend_factory(r, self.log)
        s = backend.open("rb")
        with s as source:
            if target != "-":
                self.restore_file(source, target)
            else:
                self.restore_stdout(source)

    @locked(target=".purge", mode="shared")
    def restore_file(self, source, target):
        """Bulk-copy from open revision `source` to target file."""
        self.log.debug("restore-file", source=source.name, target=target)
        open(target, "ab").close()  # touch into existence
        with open(target, "r+b", buffering=CHUNK_SIZE) as target:
            try:
                posix_fadvise(target.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            except Exception:
                pass
            copy(source, target)

    @locked(target=".purge", mode="shared")
    def restore_stdout(self, source):
        """Emit restore data to stdout (for pipe processing)."""
        self.log.debug("restore-stdout", source=source.name)
        try:
            posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
        except Exception:
            pass
        with os.fdopen(os.dup(1), "wb") as target:
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)

    @locked(target=".purge", mode="shared")
    def upgrade(self):
        """Upgrade this backup's store from cowfile to chunked.

        This can take a long time and is intended to be interruptable.

        We start creating new backups with the new format once everything
        is converted as we do not want to interfere with the config file
        but allow upgrading without a format specification.

        """
        from backy.backends.chunked import ChunkedFileBackend
        from backy.sources.file import File

        last_worklist = []

        while True:
            self.scan()
            to_upgrade = [
                r for r in self.clean_history if r.backend_type == "cowfile"
            ]
            if not to_upgrade:
                break
            if to_upgrade == last_worklist:
                self.log.error("upgrade-no-progress")
                break
            last_worklist = to_upgrade

            self.log.info("upgrade-found-new", num_revisions=len(to_upgrade))
            # Upgrade the newest then start again. The revisions may change
            # beneath us and this may cause a) new revisions to appear and b)
            # old revisions to disappear. We want to upgraded new revisions as
            # quickly as possible as having the newest upgraded means that
            # then next backup will be able to use the new format and we don't
            # have to re-upgrade it again.
            try:
                revision = to_upgrade[-1]
                self.log.info(
                    "upgrade-converting",
                    revision_uuid=revision.uuid,
                    timestamp=revision.timestamp,
                )
                original_file = revision.filename + ".old"
                if not os.path.exists(original_file):
                    # We may be resuming a partial upgrade. Only move the file
                    # if our .old doesn't exist.
                    os.rename(revision.filename, original_file)
                else:
                    self.log.info("upgrade-resuming")
                    if os.path.exists(revision.filename):
                        os.unlink(revision.filename)
                revision.writable()
                chunked = ChunkedFileBackend(revision, self.log)
                chunked.clone_parent = False
                file = File(dict(filename=original_file, cow=False), self.log)(
                    revision
                )
                # Keep a copy of the statistics as it will get replaced when
                # running the full copy.
                original_stats = revision.stats.copy()
                with file as f:
                    f.backup(chunked)
                revision.stats = original_stats
                revision.backend_type = "chunked"
                revision.write_info()
                revision.readonly()
                os.unlink(original_file)
            except Exception:
                self.log.exception("upgrade-error")
                # We may be seeing revisions getting removed, try again.
                return

            # Wait a bit, to be graceful to the host system just in case this
            # truns into a spinning loop.
            time.sleep(5)

    ######################
    # Looking up revisions

    def last_by_tag(self):
        """Return a dictionary showing the last time each tag was
        backed up.

        Tags that have never been backed up won't show up here.

        """
        last_times = {}
        for revision in self.clean_history:
            for tag in revision.tags:
                last_times.setdefault(tag, min_date())
                last_times[tag] = max([last_times[tag], revision.timestamp])
        return last_times

    def find_revisions(self, spec):
        """Get a sorted list of revisions, oldest first, that match the given
        specification.
        """
        if isinstance(spec, str) and spec.startswith("tag:"):
            tag = spec.replace("tag:", "")
            result = [r for r in self.history if tag in r.tags]
        elif spec == "all":
            result = self.history[:]
        else:
            result = [self.find(spec)]
        return result

    def find_by_number(self, spec):
        """Returns revision by relative number.

        0 is the newest,
        1 is the next older,
        2 is the even next older,
        and so on ...

        Raises IndexError or ValueError if no revision is found.
        """
        spec = int(spec)
        if spec < 0:
            raise KeyError("Integer revisions must be positive")
        return self.history[-spec - 1]

    def find_by_tag(self, spec):
        """Returns the latest revision matching a given tag.

        Raises IndexError or ValueError if no revision is found.
        """
        if spec in ["last", "latest"]:
            return self.history[-1]
        matching = [r for r in self.history if spec in r.tags]
        return max((r.timestamp, r) for r in matching)[1]

    def find_by_uuid(self, spec):
        """Returns revision matched by UUID.

        Raises IndexError if no revision is found.
        """
        try:
            return self._by_uuid[spec]
        except KeyError:
            raise IndexError()

    def find(self, spec) -> Revision:
        """Flexible revision search.

        Locates a revision by relative number, by tag, or by uuid.

        """
        if spec is None or spec == "" or not self.history:
            raise KeyError(spec)

        for find in (self.find_by_number, self.find_by_uuid, self.find_by_tag):
            try:
                return find(spec)
            except (ValueError, IndexError):
                pass
        self.log.warning("find-rev-not-found", spec=spec)
        raise KeyError(spec)
