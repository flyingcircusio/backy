import contextlib
import functools
import json
import struct
import subprocess
from collections import namedtuple
from typing import IO, BinaryIO, Iterator, Optional, cast

from structlog.stdlib import BoundLogger

from backy.ext_deps import RBD
from backy.utils import CHUNK_SIZE


class RBDClient(object):
    log: BoundLogger

    def __init__(self, log: BoundLogger):
        self.log = log.bind(subsystem="rbd")

    def _ceph_cli(self, cmdline, encoding="utf-8") -> str:
        # This wrapper function for the `rbd` command is only used for getting
        # and interpreting text messages, making this the correct level for
        # managing text encoding. Other use cases where binary data is piped
        # to rbd have their own dedicated wrappers.
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

    def exists(self, snapspec: str):
        try:
            return self._rbd(["info", snapspec], format="json")
        except subprocess.CalledProcessError as e:
            if e.returncode == 2:
                return False
            raise

    def map(self, image: str, readonly=False):
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
            SPEC = {"pool", "name", "snap", "device"}
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
    def export_diff(self, new: str, old: str) -> Iterator["RBDDiffV1"]:
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
    def image_reader(self, image: str) -> Iterator[BinaryIO]:
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


def unpack_from(fmt, f):
    size = struct.calcsize(fmt)
    b = f.read(size)
    return struct.unpack(fmt, b)


Zero = namedtuple("Zero", ["start", "length"])
Data = namedtuple("Data", ["start", "length", "stream"])
SnapSize = namedtuple("SnapSize", ["size"])
FromSnap = namedtuple("FromSnap", ["snapshot"])
ToSnap = namedtuple("ToSnap", ["snapshot"])


class RBDDiffV1(object):
    f: IO
    phase: str  # header, metadata, data
    record_type: Optional[str]
    _streaming: bool

    header = b"rbd diff v1\n"

    def __init__(self, fh):
        # self.filename = filename
        self.f = fh

        self.phase = "header"
        self.read_header()
        self.record_type = None
        self._streaming = False

    def read_header(self):
        assert self.phase == "header"
        header = self.f.read(len(self.header))
        if header != self.header:
            raise ValueError("Unexpected header: {0!r}".format(header))
        self.phase = "metadata"

    def read_record(self):
        if self.phase == "end":
            return
        assert not self._streaming, "Unread data from read_w. Consume first."
        last_record_type = self.record_type
        self.record_type = self.f.read(1).decode("ascii")
        if self.record_type not in ["f", "t", "s", "w", "z", "e"]:
            raise ValueError(
                'Got invalid record type "{}". Previous record: {}'.format(
                    self.record_type, last_record_type
                )
            )
        method = getattr(self, "read_{}".format(self.record_type))
        return method()

    def read_fbytes(self, encoding=None):
        length = unpack_from("<i", self.f)[0]
        data = self.f.read(length)
        if encoding is not None:
            data = data.decode(encoding)
        return data

    def read_f(self):
        "from snap"
        assert self.phase == "metadata"
        return FromSnap(self.read_fbytes("ascii"))

    def read_t(self):
        "to snap"
        assert self.phase == "metadata"
        return ToSnap(self.read_fbytes("ascii"))

    def read_s(self):
        "size"
        assert self.phase == "metadata"
        return SnapSize(unpack_from("<Q", self.f)[0])

    def read_e(self):
        self.phase = "end"
        return None

    def read_w(self):
        "updated data"
        if self.phase == "metadata":
            self.phase = "data"
        offset, length = unpack_from("<QQ", self.f)

        def stream():
            remaining = length
            while remaining:
                read = min(4 * 1024**2, remaining)
                chunk = self.f.read(read)
                remaining = remaining - read
                yield chunk
            self._streaming = False

        self._streaming = True
        return Data(offset, length, stream)

    def read_z(self):
        "zero data"
        if self.phase == "metadata":
            self.phase = "data"
        offset, length = unpack_from("<QQ", self.f)
        return Zero(offset, length)

    def read_metadata(self):
        while True:
            record = self.read_record()
            if self.phase != "metadata":
                self.first_data_record = record
                return
            yield record

    def read_data(self):
        if not self.first_data_record:
            return
        yield self.first_data_record
        while True:
            record = self.read_record()
            if record is None:
                return
            yield record

    def integrate(self, target, snapshot_from, snapshot_to):
        """Integrate this diff into the given target."""
        bytes = 0

        for record in self.read_metadata():
            if isinstance(record, SnapSize):
                target.truncate(record.size)
            elif isinstance(record, FromSnap):
                assert record.snapshot == snapshot_from
            elif isinstance(record, ToSnap):
                assert record.snapshot == snapshot_to

        for record in self.read_data():
            target.seek(record.start)
            if isinstance(record, Zero):
                target.write(b"\x00" * record.length)
            elif isinstance(record, Data):
                for chunk in record.stream():
                    target.write(chunk)
            bytes += record.length

        self.f.close()
        return bytes
