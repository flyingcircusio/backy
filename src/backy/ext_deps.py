"""Paths to reach external commands.

Override for testing or in custom build environments.
"""

import os
import sys

BACKY_CMD = os.environ.get('BACKY_CMD', os.path.join(
    os.getcwd(), os.path.dirname(sys.argv[0]), 'backy'))
BTRFS = os.environ.get('BACKY_BTRFS', 'btrfs')
CP = os.environ.get('BACKY_CP', 'cp')
RBD = os.environ.get('BACKY_RBD', 'rbd')
BASH = os.environ.get('BACKY_BASH', 'bash')
