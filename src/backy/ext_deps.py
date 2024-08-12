"""Paths to reach external commands.

Override for testing or in custom build environments.
"""

import os
import sys

BACKY_CLI_CMD = os.environ.get(
    "BACKY_CLI_CMD",
    os.path.join(os.getcwd(), os.path.dirname(sys.argv[0]), "backy"),
)
BACKY_RBD_CMD = os.environ.get(
    "BACKY_RBD_CMD",
    os.path.join(os.getcwd(), os.path.dirname(sys.argv[0]), "backy-rbd"),
)
BACKY_S3_CMD = os.environ.get(
    "BACKY_S3_CMD",
    os.path.join(os.getcwd(), os.path.dirname(sys.argv[0]), "backy-s3"),
)
CP = os.environ.get("BACKY_CP", "cp")
RBD = os.environ.get("BACKY_RBD", "rbd")
BACKY_EXTRACT = os.environ.get("BACKY_EXTRACT", "backy-extract")
BASH = os.environ.get("BACKY_BASH", "bash")
