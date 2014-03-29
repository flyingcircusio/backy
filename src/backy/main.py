# -*- encoding: utf-8 -*-

# from fadvise import posix_fadvise, POSIX_FADV_SEQUENTIAL, POSIX_FADV_DONTNEED
import argparse
import os
import sys
import backy.operations
import logging


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true',  help='verbose output')

    subparsers = parser.add_subparsers()

    # backup
    p = subparsers.add_parser(
        'backup',
        help="""\
Perform a backup.
""")
    p.set_defaults(func=backy.operations.backup)
    p.add_argument('source')
    p.add_argument('target')

    # restore
    p = subparsers.add_parser(
        'restore',
        help="""\
Perform a backup.
""")
    p.set_defaults(func=backy.operations.restore)
    p.add_argument('-r', '--revision', default=0, type=int)
    p.add_argument('-n', '--checkonly',
                   action='store_true', help='check only')
    p.add_argument('-f', '--force',
                   action='store_true', help='force, overwrite existing files')
    p.add_argument('source')
    p.add_argument('target')

    # scrub
    p = subparsers.add_parser(
        'scrub',
        help="""\
Verify all blocks of the backup against their checksums.
""")
    p.set_defaults(func=backy.operations.scrub)
    p.add_argument('target')

    # clean
    p = subparsers.add_parser(
        'clean',
        help="""\
Remove old incrementals.
""")
    p.set_defaults(func=backy.operations.clean)
    p.add_argument('-r', '--revision', default=0, type=int)
    p.add_argument('-f', '--force',
                   action='store_true', help='force, overwrite existing files')
    p.add_argument('target')

    # ls
    p = subparsers.add_parser(
        'ls',
        help="""\
List contents of backup.
""")
    p.set_defaults(func=backy.operations.ls)
    p.add_argument('target')

    args = parser.parse_args()

    # Consume global arguments
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        stream=sys.stdout, level=level, format='%(message)s')

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']

    try:
        args.func(**func_args)
        # XXX print stats
        sys.exit(0)
    except Exception, e:
        logging.error('Unexpected exception')
        logging.exception(e)
        os._exit(1)
