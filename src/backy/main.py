# -*- encoding: utf-8 -*-

from backy.utils import format_timestamp, format_bytes_flexible
from prettytable import PrettyTable
import argparse
import backy.backup
import backy.daemon
import datetime
import errno
import faulthandler
import logging
import logging.handlers
import signal
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


def valid_date(s):
    if s is None:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


class Command(object):
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, path):
        self.path = path

    def init(self, type, source):
        backy.backup.Backup.init(self.path, type, source)

    def status(self):
        b = backy.backup.Backup(self.path)
        total_bytes = 0

        t = PrettyTable(['Date (UTC)', 'ID', 'Size', 'Durat', 'Tags', 'Trust'])
        t.align = 'l'
        t.align['Size'] = 'r'
        t.align['Durat'] = 'r'

        for r in b.history:
            total_bytes += r.stats.get('bytes_written', 0)
            t.add_row([format_timestamp(r.timestamp).replace(' UTC', ''),
                       r.uuid,
                       format_bytes_flexible(r.stats.get('bytes_written', 0)),
                       str(round(r.stats.get('duration', 0), 1)) + ' s',
                       ','.join(r.tags),
                       r.trust])

        print(t)

        print('{} revisions containing {} data (estimated)'.format(
            len(b.history),
            format_bytes_flexible(total_bytes)))

    def backup(self, tags):
        b = backy.backup.Backup(self.path)
        b._clean()
        try:
            tags = set(t.strip() for t in tags.split(','))
            b.backup(tags)
        except IOError as e:
            if e.errno not in [errno.EDEADLK, errno.EAGAIN]:
                raise
            logger.info('Backup already in progress.')
        finally:
            b._clean()

    def restore(self, revision, target):
        b = backy.backup.Backup(self.path)
        b.restore(revision, target)

    def find(self, revision):
        b = backy.backup.Backup(self.path)
        print(b.find(revision).filename)

    def scheduler(self, config):
        backy.daemon.main(config)

    def check(self, config):
        backy.daemon.check(config)

    def purge(self):
        b = backy.backup.Backup(self.path)
        b.purge()

    def nbd(self, host, port):
        b = backy.backup.Backup('.')
        b.nbd_server(host, port)

    def upgrade(self):
        b = backy.backup.Backup('.')
        b.upgrade()

    def distrust(self, revision, from_, until):
        b = backy.backup.Backup('.')
        b.distrust(revision, from_, until)

    def verify(self, revision):
        b = backy.backup.Backup('.')
        b.verify(revision)


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

    # BACKUP
    p = subparsers.add_parser(
        'purge',
        help="""\
Purge the backup store (i.e. chunked) from unused data.
""")
    p.set_defaults(func='purge')

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

    # NBD
    p = subparsers.add_parser('nbd-server', help="""\
Serve the revisions of this backup through an NBD server.
""")
    p.add_argument('-H', '--host', default='127.0.0.1',
                   help='which IP address to listen on (default: 127.0.0.1)')
    p.add_argument('-p', '--port', default='9000', type=int,
                   help='which port to listen on (default: 9000)')

    p.set_defaults(func='nbd')

    # upgrade
    p = subparsers.add_parser('upgrade', help="""\
Upgrade this backup (incl. its data) to the newest supported version.
""")
    p.set_defaults(func='upgrade')

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

    # DISTRUST
    p = subparsers.add_parser(
        'distrust',
        help="""\
Distrust one or all revisions.
""")
    p.add_argument('-r', '--revision', metavar='SPEC', default='',
                   help='use revision SPEC to distrust, '
                        'distrusting all if not given')
    p.add_argument('-f', '--from', metavar='DATE',
                   type=valid_date,
                   help='Mark revisions on or after this date as distrusted',
                   dest='from_')
    p.add_argument('-u', '--until', metavar='DATE',
                   type=valid_date,
                   help='Mark revisions on or before this date as distrusted')
    p.set_defaults(func='distrust')

    # VERIFY
    p = subparsers.add_parser(
        'verify',
        help="""\
Verify one or all revisions.
""")
    p.add_argument('-r', '--revision', metavar='SPEC', default='',
                   help='use revision SPEC to verify, '
                        'verifying all if not given')
    p.set_defaults(func='verify')

    return parser


def main(enable_fault_handler=True):
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

    # Process inspection
    if enable_fault_handler:
        faulthandler.enable()
        faulthandler.register(signal.SIGUSR2)

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
        print('Error: {}'.format(e), file=sys.stderr)
        logger.exception(e)
        logger.info('Backy operation failed.')
        sys.exit(1)
