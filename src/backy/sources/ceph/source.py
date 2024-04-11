import time

from structlog.stdlib import BoundLogger

import backy.backends
import backy.utils
from backy.revision import Revision, Trust

from ...backends import BackyBackend
from ...quarantine import QuarantineReport
from .. import BackySource, BackySourceContext, BackySourceFactory
from .rbd import RBDClient


class CephRBD(BackySource, BackySourceFactory, BackySourceContext):
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

    def __init__(self, config: dict, log: BoundLogger):
        self.pool = config["pool"]
        self.image = config["image"]
        self.always_full = config.get("full-always", False)
        self.log = log.bind(subsystem="ceph")
        self.rbd = RBDClient(self.log)

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

    def __call__(self, revision: Revision):
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

    def backup(self, target: BackyBackend) -> None:
        if self.always_full:
            self.log.info("backup-always-full")
            self.full(target)
            return
        revision = self.revision
        while True:
            parent = revision.get_parent()
            if not parent:
                self.log.info("backup-no-valid-parent")
                self.full(target)
                return
            if not self.rbd.exists(self._image_name + "@backy-" + parent.uuid):
                self.log.info(
                    "ignoring-rev-without-snapshot",
                    revision_uuid=parent.uuid,
                )
                revision = parent
                continue
            # Ok, it's trusted and we have a snapshot. Let's do a diff.
            break
        self.diff(target, parent)

    def diff(self, target: BackyBackend, parent: Revision) -> None:
        self.log.info("diff")
        snap_from = "backy-" + parent.uuid
        snap_to = "backy-" + self.revision.uuid
        s = self.rbd.export_diff(self._image_name + "@" + snap_to, snap_from)
        with s as source, target.open("r+b", parent) as target_:
            bytes = source.integrate(target_, snap_from, snap_to)
        self.log.info("diff-integration-finished")

        self.revision.stats["bytes_written"] = bytes

        # TMP Gather statistics to see where to optimize
        from backy.backends.chunked.chunk import chunk_stats

        self.revision.stats["chunk_stats"] = chunk_stats

    def full(self, target: BackyBackend) -> None:
        self.log.info("full")
        s = self.rbd.export(
            "{}/{}@backy-{}".format(self.pool, self.image, self.revision.uuid)
        )
        copied = 0
        with s as source, target.open("r+b") as target_:
            while True:
                buf = source.read(4 * backy.utils.MiB)
                if not buf:
                    break
                target_.write(buf)
                copied += len(buf)
        self.revision.stats["bytes_written"] = copied

        # TMP Gather statistics to see if we actually are aligned.
        from backy.backends.chunked.chunk import chunk_stats

        self.revision.stats["chunk_stats"] = chunk_stats

    def verify(self, target: BackyBackend) -> bool:
        s = self.rbd.image_reader(
            "{}/{}@backy-{}".format(self.pool, self.image, self.revision.uuid)
        )
        self.revision.stats["ceph-verification"] = "partial"

        with s as source, target.open("rb") as target_:
            self.log.info("verify")
            return backy.utils.files_are_roughly_equal(
                source,
                target_,
                report=lambda s, t, o: self.revision.backup.quarantine.add_report(
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
        if not self.always_full and self.revision.backup.local_history:
            keep_snapshot_revision = self.revision.backup.local_history[-1].uuid
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
