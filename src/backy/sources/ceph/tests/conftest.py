import argparse
import json
import subprocess

import pytest

from backy.sources.ceph.rbd import RBDClient
import backy.sources.ceph


class CephCLIBase:
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.parser = parser = argparse.ArgumentParser()

        subparsers = parser.add_subparsers()

        # parser.add_argument("--version", action="store_true")
        version_ = subparsers.add_parser("version")
        version_.set_defaults(func="version")

        map_ = subparsers.add_parser("map")
        map_.add_argument("snapspec")
        map_.add_argument("--read-only", action="store_true")
        map_.set_defaults(func="map")

        unmap_ = subparsers.add_parser("unmap")
        unmap_.add_argument("device")
        unmap_.set_defaults(func="unmap")

        showmapped = subparsers.add_parser("showmapped")
        showmapped.add_argument("--format", choices=["json"])
        showmapped.set_defaults(func="showmapped")

        info = subparsers.add_parser("info")
        info.add_argument("imagespec")
        info.add_argument("--format", choices=["json"])
        info.set_defaults(func="info")

        snap = subparsers.add_parser("snap")
        snap_sub = snap.add_subparsers()
        snap_create = snap_sub.add_parser("create")
        snap_create.add_argument("snapspec")
        snap_create.set_defaults(func="snap_create")
        snap_ls = snap_sub.add_parser("ls")
        snap_ls.add_argument("imagespec")
        snap_ls.add_argument("--format", choices=["json"])
        snap_ls.set_defaults(func="snap_ls")
        snap_rm = snap_sub.add_parser("rm")
        snap_rm.add_argument("snapspec")
        snap_rm.set_defaults(func="snap_rm")

        self.images = {}
        self.snaps = {}
        self._freeze_mapped = (
            False  # flag for stopping modifications to images and snaps
        )

    def __call__(self, cmdline) -> str:
        print(cmdline)
        assert cmdline[0] == "rbd"
        cmdline.pop(0)

        def prep_cmdline(arg):
            if arg == "--version":
                arg = "version"
            return arg

        cmdline = map(prep_cmdline, cmdline)
        args = self.parser.parse_args(cmdline)
        func = getattr(self, args.func)
        args = dict(args._get_kwargs())
        del args["func"]
        return func(**args)

    def version(self):
        ...

    def map(self, snapspec, read_only):
        ...

    # implementation restriction: `rbd unmap` takes imagespecs, snapspecs, or devices
    # as args, AFAIK we only use devices as args in backy for now
    def unmap(self, device, read_only):
        ...

    def showmapped(self, format):
        assert format == "json"
        return json.dumps(self.mapped_images)

    def info(self, imagespec, format):
        assert format == "json"
        if imagespec not in self.images:
            raise subprocess.CalledProcessError(cmd="info", returncode=2)

    def snap_create(self, snapspec):
        image, snapname = snapspec.split("@")
        if not self._freeze_mapped:
            try:
                self.snaps[image].append(
                    {
                        # example snapshot data
                        "id": 86925,
                        "name": snapname,
                        "size": 32212254720,
                        "protected": "false",
                        "timestamp": "Sun Feb 12 18:35:18 2023",
                    }
                )
            except KeyError:
                raise subprocess.CalledProcessError(cmd="snap create", returncode=2)

    def snap_ls(self, format, imagespec):
        assert format == "json"
        return json.dumps(self.snaps[imagespec])

    def snap_rm(self, snapspec):
        image, snapname = snapspec.split("@")
        if not self._freeze_mapped:
            try:
                for candidate in self.snaps[image]:
                    if candidate["name"] == snapname:
                        self.snaps[image].remove(candidate)
                        break
                else:
                    raise subprocess.CalledProcessError(
                        cmd="snap rm", returncode=2
                    )
            except KeyError:
                raise subprocess.CalledProcessError(cmd="snap rm", returncode=2)

    def _register_image_for_snaps(self, imagespec):
        """helper function to register an image as present in test setup, such that it
        can receive snapshots.
        Reason: `rbd snap` fails for snapshot operations on images that do not exist."""
        self.snaps[imagespec] = []

    @staticmethod
    def _parse_snapspec(snapspec):
        imagedata = {}
        try:
            imagespec, snapname = snapspec.split("@")
            imagedata["snap"] = snapname
        except ValueError:
            # no snapshot specified
            imagespec = snapspec
            snapname = None
        pool, imagename = imagespec.split("/")
        imagedata["pool"] = pool
        imagedata["name"] = imagename
        return imagedata

class CephJewelCLI(CephCLIBase):
    def __init__(self, tmpdir):
        super().__init__(tmpdir)
        self.mapped_images = {}

    def version(self):
        # This isn't really what happens in upstream but due to the way
        # we built it on NixOS. Don't hurt the Ceph people.
        return "ceph version Development (no_version)"

    def unmap(self, device):
        if not self._freeze_mapped:
            for k, v in list(self.mapped_images.items()):
                if device == v["device"]:
                    del self.mapped_images[k]
                    break
            else:
                raise subprocess.CalledProcessError(cmd="unmap", returncode=2)

    def map(self, snapspec, read_only):
        image = self._parse_snapspec(snapspec)
        id = len(self.mapped_images)
        image["device"] = f"{self.tmpdir}/rbd{id}"
        if not self._freeze_mapped:
            with open(image["device"], "a"):
                pass
            self.mapped_images[str(id)] = image
        return ""


class CephLuminousCLI(CephJewelCLI):
    def version(self):
        return "ceph version Development (no_version) luminous (stable)"


class CephNautilusCLI(CephCLIBase):
    def __init__(self, tmpdir):
        super().__init__(tmpdir)
        self.mapped_images = []

    def version(self):
        return "ceph version 14.2.22 (ca74598065096e6fcbd8433c8779a2be0c889351) nautilus (stable)"

    def map(self, snapspec, read_only):
        image = self._parse_snapspec(snapspec)
        id = len(self.mapped_images)
        image["id"] = str(id)
        image["namespace"] = ""
        image["device"] = f"{self.tmpdir}/rbd{id}"
        if not self._freeze_mapped:
            with open(image["device"], "a"):
                pass
            self.mapped_images.append(image)
        return ""

    def unmap(self, device):
        if not self._freeze_mapped:
            for candidate in list(self.mapped_images):
                if device == candidate["device"]:
                    self.mapped_images.remove(candidate)
                    break
            else:
                raise subprocess.CalledProcessError(cmd="unmap", returncode=2)


@pytest.fixture(params=[CephJewelCLI, CephLuminousCLI, CephNautilusCLI])
def rbdclient(request, tmpdir, monkeypatch):
    monkeypatch.setattr(
        backy.sources.ceph, "CEPH_RBD_SUPPORTS_WHOLE_OBJECT_DIFF", True
    )

    client = RBDClient()
    client._ceph_cli = request.param(tmpdir)

    return client
