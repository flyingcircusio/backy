from subprocess import PIPE, run


def detect_whole_object_support():
    result = run(
        ["rbd", "help", "export-diff"], stdout=PIPE, stderr=PIPE, check=True
    )
    return "--whole-object" in result.stdout.decode("ascii")


try:
    CEPH_RBD_SUPPORTS_WHOLE_OBJECT_DIFF = detect_whole_object_support()
except Exception:
    CEPH_RBD_SUPPORTS_WHOLE_OBJECT_DIFF = False
