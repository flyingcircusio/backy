# -*- encoding: utf-8 -*-

import argparse
import backy.backup
import logging
import os
import sys


logger = logging.getLogger(__name__)


def init_logging(backupdir, verbose=False):
    if verbose:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(
        filename=os.path.join(backupdir, 'backy.log'),
        format='%(asctime)s [%(process)d] %(message)s',
        level=logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    logging.getLogger('').addHandler(console)

    logger.info('$ ' + ' '.join(sys.argv))


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
    p.add_argument(
        '-f', '--force', default='',
        help='Force backup of a given tag, even if not required by schedule.')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="""\
Restore (a given revision) to a given target.
""")
    p.add_argument('-r', '--revision', default='latest')
    p.add_argument('target')
    p.set_defaults(func='restore')

    # STATUS
    p = subparsers.add_parser(
        'status',
        help="""\
Show backup status. Show inventory and summary information.
""")
    p.set_defaults(func='status')

    # SCHEDULE SIMULATION
    p = subparsers.add_parser(
        'schedule',
        help="""\
Simulate the schedule.
""")
    p.set_defaults(func='schedule')
    p.add_argument('days', type=int)

    args = parser.parse_args()

    init_logging(args.backupdir, args.verbose)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    commands = backy.backup.Commands(args.backupdir)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        logger.info('Backup complete.\n')
        sys.exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        logger.info('Backup failed.\n')
        os._exit(1)
