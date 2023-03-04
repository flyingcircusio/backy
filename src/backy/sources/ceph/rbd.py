import contextlib
import json
import logging
import subprocess

import backy.sources.ceph

from ...ext_deps import RBD
from ...utils import CHUNK_SIZE
from .diff import RBDDiffV1

logger = logging.getLogger(__name__)


class RBDClient(object):
    def _ceph_cli(self, cmdline):
        return subprocess.check_output(cmdline)

    def _rbd(self, cmd, format=None):
        cmd = filter(None, cmd)
        rbd = [RBD]

        rbd.extend(cmd)

        if format == "json":
            rbd.append("--format=json")

        logger.debug(" ".join(rbd))
        result = self._ceph_cli(rbd)

        if format == "json":
            result = json.loads(result.decode("utf-8"))

        return result

    def exists(self, image):
        try:
            return self._rbd(["info", image], format="json")
        except subprocess.CalledProcessError as e:
            if e.returncode == 2:
                return False
            raise

    def map(self, image, readonly=False):
        def parse_mappings_pre_nautilus(mappings):
            """The parser code for Ceph release Luminous and earlier."""
            for mapping in mappings.values():
                if image == "{pool}/{name}@{snap}".format(**mapping):
                    return mapping
            raise RuntimeError("Map not found in mapping list.")

        def parse_mappings_since_nautilus(mappings):
            """The parser code for Ceph release Nautilus and later."""
            for mapping in mappings:
                if image == "{pool}/{name}@{snap}".format(**mapping):
                    return mapping
            raise RuntimeError("Map not found in mapping list.")

        versionstring = self._rbd(["--version"])

        self._rbd(["map", image, "--read-only" if readonly else ""])

        mappings_raw = self._rbd(["showmapped"], format="json")

        if "nautilus" in versionstring:
            mapping = parse_mappings_since_nautilus(mappings_raw)
        elif "luminous" in versionstring:
            mapping = parse_mappings_pre_nautilus(mappings_raw)
        else:
            # our jewel build provides no version info
            # this will break with releases newer than nautilus
            mapping = parse_mappings_pre_nautilus(mappings_raw)

        def scrub_mapping(mapping):
            SPEC = set(["pool", "name", "snap", "device"])
            # Ensure all specced keys exist
            for key in SPEC:
                if key not in mapping:
                    raise KeyError(
                        f"Missing key `{key}` in mapping {mapping!r}"
                    )
            # Scrub all non-specced keys
            for key in list(mapping):
                if key not in SPEC:
                    del mapping[key]
            return mapping

        return scrub_mapping(mapping)

    def unmap(self, device):
        self._rbd(["unmap", device])

    def snap_create(self, image):
        self._rbd(["snap", "create", image])

    def snap_ls(self, image):
        return self._rbd(["snap", "ls", image], format="json")

    def snap_rm(self, image):
        return self._rbd(["snap", "rm", image])

    @contextlib.contextmanager
    def export_diff(self, new, old):
        logger.info(str(backy.sources.ceph.CEPH_VERSION))
        if backy.sources.ceph.CEPH_RBD_SUPPORTS_WHOLE_OBJECT_DIFF:
            EXPORT_WHOLE_OBJECT = ["--whole-object"]
        else:
            EXPORT_WHOLE_OBJECT = []
        proc = subprocess.Popen(
            [RBD, "export-diff", new, "--from-snap", old]
            + EXPORT_WHOLE_OBJECT
            + ["-"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            # Have a rather largish buffer size, so rbd has some room to
            # push its data to, when we are busy writing.
            bufsize=8 * CHUNK_SIZE,
        )
        try:
            yield RBDDiffV1(proc.stdout)
        finally:
            proc.stdout.close()
            proc.wait()

    @contextlib.contextmanager
    def image_reader(self, image):
        mapped = self.map(image, readonly=True)
        source = open(mapped["device"], "rb", buffering=CHUNK_SIZE)
        try:
            yield source
        finally:
            source.close()
            self.unmap(mapped["device"])

    @contextlib.contextmanager
    def export(self, image):
        proc = subprocess.Popen(
            [RBD, "export", image, "-"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            # Have a rather largish buffer size, so rbd has some room to
            # push its data to, when we are busy writing.
            bufsize=4 * CHUNK_SIZE,
        )
        try:
            yield proc.stdout
        finally:
            proc.stdout.close()
            proc.wait()
