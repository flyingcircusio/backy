import contextlib
import functools
import json
import subprocess
from typing import IO, Iterator, cast

from structlog.stdlib import BoundLogger

from ...ext_deps import RBD
from ...utils import CHUNK_SIZE
from .diff import RBDDiffV1


class RBDClient(object):
    log: BoundLogger

    def __init__(self, log: BoundLogger):
        self.log = log.bind(subsystem="rbd")

    def _ceph_cli(self, cmdline, encoding="utf-8") -> str:
        # This wrapper function for the `rbd` command is only used for
        # getting and interpreting text messages, making this the correct level for
        # managing text encoding.
        # Other use cases where binary data is piped to rbd have their own dedicated
        # wrappers.
        return subprocess.check_output(
            cmdline, encoding=encoding, errors="replace"
        )

    def _rbd(self, cmd, format=None):
        rbd = [RBD, *filter(None, cmd)]

        if format == "json":
            rbd.append("--format=json")

        self.log.debug("executing-command", command=" ".join(rbd))
        result = self._ceph_cli(rbd)

        self.log.debug("executed-command", stdout=result)
        if format == "json":
            result = json.loads(result)

        return result

    @contextlib.contextmanager
    def _rbd_stream(self, cmd: list[str]) -> Iterator[IO[bytes]]:
        rbd = [RBD, *filter(None, cmd)]

        self.log.debug("executing-command", command=" ".join(rbd))
        proc = subprocess.Popen(
            rbd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            # Have a rather largish buffer size, so rbd has some room to
            # push its data to, when we are busy writing.
            bufsize=8 * CHUNK_SIZE,
        )
        stdout = cast(IO[bytes], proc.stdout)
        try:
            yield stdout
        finally:
            stdout.close()
            rc = proc.wait()
            self.log.debug("executed-command", command=" ".join(rbd), rc=rc)
            if rc:
                raise subprocess.CalledProcessError(rc, proc.args)

    @functools.cached_property
    def _supports_whole_object(self):
        return "--whole-object" in self._rbd(["help", "export-diff"])

    def exists(self, snapspec):
        try:
            return self._rbd(["info", snapspec], format="json")
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
    def export_diff(self, new: str, old: str) -> Iterator[RBDDiffV1]:
        if self._supports_whole_object:
            EXPORT_WHOLE_OBJECT = ["--whole-object"]
        else:
            EXPORT_WHOLE_OBJECT = []
        with self._rbd_stream(
            [
                "export-diff",
                new,
                "--from-snap",
                old,
                *EXPORT_WHOLE_OBJECT,
                "-",
            ]
        ) as stdout:
            yield RBDDiffV1(stdout)

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
    def export(self, image: str) -> Iterator[IO[bytes]]:
        with self._rbd_stream(["export", image, "-"]) as stdout:
            yield stdout
