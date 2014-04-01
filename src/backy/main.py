# -*- encoding: utf-8 -*-

import argparse
import backy.backup
import logging
import os
import sys


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true',  help='verbose output')
    parser.add_argument(
        '-b', '--backupdir', default='.')

    subparsers = parser.add_subparsers()

    # INIT
    p = subparsers.add_parser(
        'init',
        help="""\
Initialize backup for a <source> in the backup directory.
""")
    p.set_defaults(func='init')
    p.add_argument('source')

    # BACKUP
    p = subparsers.add_parser(
        'backup',
        help="""\
Perform a backup.
""")
    p.set_defaults(func='backup')

    # STATUS
    p = subparsers.add_parser(
        'status',
        help="""\
Show backup status. Show inventory and summary information.
""")
    p.set_defaults(func='status')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="""\
Perform a backup.
""")
    p.set_defaults(func='restore')
    p.add_argument('-r', '--revision', default='last')
    p.add_argument('target', default=None)

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="""\
Verify all blocks a revision against their checksums.
""")
    p.set_defaults(func='scrub')
    p.add_argument('-m', '--markbad',
                   action='store_true',
                   help='Persistently mark blocks as bad.')
    p.add_argument('revision', default='all')

    # MAINTENANCE
    p = subparsers.add_parser(
        'maintenance',
        help="""\
Perform maintenance: merge deltas and remove old backups according to schedule.
""")
    p.set_defaults(func='maintenance')
    p.add_argument('-k', '--keep', default=1, type=int)

    # MOUNT
    p = subparsers.add_parser(
        'mount',
        help="""\
FUSE-Mount the backup to get access to mountable block-image files
from old revisions.
""")
    p.set_defaults(func='mount')
    p.add_argument('mountpoint')

    args = parser.parse_args()

    # Consume global arguments
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        stream=sys.stdout, level=level, format='%(message)s')

    backup = backy.backup.Backup(args.backupdir)
    func = getattr(backup, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']

    try:
        func(**func_args)
        sys.exit(0)
    except Exception, e:
        logging.error('Unexpected exception')
        logging.exception(e)
        os._exit(1)
