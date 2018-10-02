from subprocess import run, PIPE
import packaging.version

CEPH_VERSION = None


def get_ceph_major_version():
    result = run(['ceph', '-v'], stdout=PIPE, stderr=PIPE, check=True)
    version_string = result.stdout.decode('ascii').splitlines()[0]
    version_parts = version_string.split()
    if version_parts[:2] != ['ceph', 'version']:
        raise ValueError(
            'Unexpected version line: {:r}'.format(version_string))
    return packaging.version.parse(version_parts[3])

CEPH_VERSION = get_ceph_major_version()
