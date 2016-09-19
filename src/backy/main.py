# -*- encoding: utf-8 -*-

from backy.utils import format_timestamp, format_bytes_flexible
from prettytable import PrettyTable
import argparse
import backy.backup
import backy.daemon
import errno
import logging
import logging.handlers
import sys


logger = logging.getLogger(__name__)


def init_logging(logfile, verbose):  # pragma: no cover
    if logfile:
        handler = logging.handlers.WatchedFileHandler(logfile, delay=True)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(process)d] %(levelname)s %(message)s',
            '%Y-%m-%d %H:%M:%S'))
    else:
        handler = logging.StreamHandler(sys.stdout)
    # silence telnet3 logging, which logs as root logger (we don't)
    logging.getLogger().setLevel(logging.WARNING)
    toplevel = logging.getLogger('backy')
    toplevel.setLevel(logging.DEBUG if verbose else logging.INFO)
    toplevel.addHandler(handler)
    if logfile:
        logger.info('$ %s', ' '.join(sys.argv))


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self._backup = backy.backup.Backup(path)

    def init(self, type, source):
        self._backup.init(type, source)

    def status(self):
        self._backup.configure()
        total_bytes = 0

        t = PrettyTable(['Date (UTC)', 'ID', 'Size', 'Durat', 'Tags'])
        t.align = 'l'
        t.align['Size'] = 'r'
        t.align['Durat'] = 'r'

        for r in self._backup.archive.history:
            total_bytes += r.stats.get('bytes_written', 0)
            t.add_row([format_timestamp(r.timestamp).replace(' UTC', ''),
                       r.uuid,
                       format_bytes_flexible(r.stats.get('bytes_written', 0)),
                       str(round(r.stats.get('duration', 0), 1)) + ' s',
                       ','.join(r.tags)])

        print(t)

        print('{} revisions containing {} data (estimated)'.format(
            len(self._backup.archive.history),
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

    def find(self, revision):
        self._backup.configure()
        print(self._backup.find(revision))

    def scheduler(self, config):
        backy.daemon.main(config)

    def check(self, config):
        backy.daemon.check(config)


def setup_argparser():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-l', '--logfile', help='file name to write log output in. '
        'If no file name is specified, log to stdout.')
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
    p.add_argument('-r', '--revision', metavar='SPEC', default='latest',
                   help='use revision SPEC as restore source')
    p.add_argument('target', metavar='TARGET', help="""\
Copy backed up revision to TARGET. Use stdout if TARGET is "-".
""")
    p.set_defaults(func='restore')

    # FIND
    p = subparsers.add_parser('find', help="""\
Print full path to a given revision's image file.
""")
    p.add_argument('-r', '--revision', metavar='SPEC', default='latest',
                   help='use revision SPEC as restore source')
    p.set_defaults(func='find')

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
    return parser


def main():
    parser = setup_argparser()
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    # Logging
    if args.func != 'check':
        init_logging(args.logfile, args.verbose)

    command = Command(args.backupdir)
    func = getattr(command, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['backupdir']
    del func_args['logfile']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        if args.logfile:
            logger.info('Backy operation complete.')
        sys.exit(0)
    except Exception as e:
        # at least a *bit* of output to stderr in this case
        print('Error: {}'.format(e), file=sys.stderr)
        if args.logfile:
            logger.exception(e)
            logger.info('Backy operation failed.')
        sys.exit(1)
