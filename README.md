## Synopsis

    backy --help
    backy [-b <backupdir>] auto
    backy [-b <backupdir>] status
    backy [-b <backupdir>] backup
    backy [-b <backupdir>] clean [-r|--revision <revision>]
    backy [-b <backupdir>] restore [-r|--revision <revision>] [target]
    backy [-b <backupdir>] scrub [-n|--checkonly]
    backy [-b <backupdir>] init

## Restore

Restore latest image:

    $ backy restore

Restore previous image:

    $ backy restore -r 1

## Description

Backy backs up block devices (or large files) into archives and keeps
"reverse incremental" deltas of older versions.

It supports regular verification of the stored data and is intended to be
as simple as possible, support disaster recovery even with basic system tools
(not using backy) and be easy to monitor.

Once configured, everything you should need to do is run this as a cron-job:

    $ backy -b /srv/backup/mynode auto


### Backup sub-command

XXX

### Clean sub-command

XXX

### Restore sub-command

XXX

### Scrub sub-command

XXX

### List sub-command

XXX

## Examples

XXX

## Exit status

    0: Command worked properly.
    1: An error occured.

## Authors

* Daniel Kraft <daniel.kraft@d9t.de>
* Christian Theune <ct@gocept.com>

## License

GPLv3

## Hacking

    $ hg clone https://bitbucket.org/ctheune/backy
    $ cd backy
    $ virtualenv --python=python2.7 .
    $ bin/python bootstrap.py
    $ bin/buildout
    $ bin/py.test
    $ ./smoketest.sh
