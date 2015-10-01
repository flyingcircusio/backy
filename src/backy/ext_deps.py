"""Paths to reach external commands.

Override for testing or in custom build environments.
"""

import os
import sys

BACKY_CMD = os.environ.get('BACKY_CMD', os.path.join(
    os.getcwd(), os.path.dirname(sys.argv[0]), 'backy'))
BACKY_BTRFS = os.environ.get('BACKY_BTRFS', 'btrfs')
BACKY_CP = os.environ.get('BACKY_CP', 'cp')
BACKY_RBD = os.environ.get('BACKY_RBD', 'rbd')
