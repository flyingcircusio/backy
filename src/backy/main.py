# -*- encoding: utf-8 -*-

from backy.utils import format_timestamp, format_bytes_flexible
from prettytable import PrettyTable
import argparse
import backy.backup
import backy.scheduler
import errno
import logging
import os
import sys


logger = logging.getLogger(__name__)


def init_logging(backupdir, console_level):  # pragma: no cover
    logging.basicConfig(
        filename=os.path.join(backupdir, 'backy.log'),
        format='%(asctime)s [%(process)d] %(message)s',
        level=logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(console_level)
    logging.getLogger('').addHandler(console)

    logger.info('$ ' + ' '.join(sys.argv))


class Commands(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self._backup = backy.backup.Backup(path)

    def init(self, type_, source):
        self._backup.init(type_, source)

    def status(self):
        self._backup.configure()
        total_bytes = 0

        t = PrettyTable(["Date", "ID", "Size", "Duration", "Tags"])
        t.align = 'l'
        t.align['Size'] = 'r'
        t.align['Duration'] = 'r'

        for r in self._backup.archive.history:
            total_bytes += r.stats.get('bytes_written', 0)
            t.add_row([format_timestamp(r.timestamp),
                       r.uuid,
                       format_bytes_flexible(r.stats.get('bytes_written', 0)),
                       int(r.stats.get('duration', 0)),
                       ', '.join(r.tags)])

        print(t)

        print("== Summary")
        print("{} revisions".format(len(self._backup.archive.history)))
        print("{} data (estimated)".format(
            format_bytes_flexible(total_bytes)))

    def backup(self, tags):
        self._backup.configure()
        try:
            self._backup.backup(tags)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            logger.info('Backup already in progress.')

    def restore(self, revision, target):
        self._backup.configure()
        self._backup.restore(revision, target)

    def scheduler(self, config):
        backy.scheduler.main(config)

    def check(self, config):
        backy.scheduler.check(config)


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-b', '--backupdir', default='.',
        help='directory where backups and logs are written to '
        '(default: %(default)s)')

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
        'tags',
        help='Tags to apply to the backup.')
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

    # SCHEDULER DAEMON
    p = subparsers.add_parser(
        'scheduler',
        help="""\
Run the scheduler.
""")
    p.set_defaults(func='scheduler')
    p.add_argument(
        '-c', '--config', default='/etc/backy.conf')

    # SCHEDULE CHECK
    p = subparsers.add_parser(
        'check',
        help="""\
Check whether all jobs adhere to their schedules' SLA.
""")
    p.set_defaults(func='check')
    p.add_argument(
        '-c', '--config', default='/etc/backy.conf')

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    if args.func != 'check':
        if args.verbose:
            console_level = logging.DEBUG
        elif args.func == 'scheduler':
            console_level = logging.INFO
        else:
            console_level = logging.WARNING
        init_logging(args.backupdir, console_level)

    commands = Commands(args.backupdir)
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
        sys.exit(1)
