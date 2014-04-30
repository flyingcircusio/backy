# -*- encoding: utf-8 -*-

import argparse
import backy.backup
import logging
import os
import sys


logger = logging.getLogger(__name__)


def init_logging(verbose=False):
    if verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        filename='backy.log',
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        level=logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    logging.getLogger('').addHandler(console)

    logger.debug(' '.join(sys.argv))


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
    p.add_argument('type')
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

    # MAINTENANCE
    p = subparsers.add_parser(
        'maintenance',
        help="""\
Perform maintenance: remove old backups according to schedule.
""")
    p.set_defaults(func='maintenance')
    p.add_argument('-k', '--keep', default=1, type=int)

    args = parser.parse_args()

    init_logging(args.verbose)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    backup = backy.backup.Backup(args.backupdir)
    func = getattr(backup, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        sys.exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        os._exit(1)
