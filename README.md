**This is work in progress. Parts of this file are currently fiction.**

## Synopsis

    backy --help
    backy [-b <backupdir>] init
    backy [-b <backupdir>] backup
    backy [-b <backupdir>] status
    backy [-b <backupdir>] restore [-r|--revision <revision>] [target]
    backy [-b <backupdir>] scrub [-n|--checkonly]
    backy [-b <backupdir>] maintenance
    backy [-b <backupdir>] mount <mountpoint>

## Restore

Restore latest image back to the original source:

    $ backy restore 

Restore previous image:

    $ backy restore -r 1

## Description

Backy backs up block devices (or large files) into archives and keeps
"reverse incremental" deltas of older versions.

It supports regular verification of the stored data and is intended to be
as simple as possible, support disaster recovery even with basic system tools
(not using backy) and be easy to monitor.

Once configured, everything you should need to do is run two cronjobs
regularly:

    $ backy -b /srv/backup/mynode backup
    $ backy -b /srv/backup/mynode maintenance

### Backup sub-command

Check whether a backup is needed according to schedule. If it is,
then create a new full backup and turn the last backup into a delta.

If no backup is needed, just exit silently.

### Maintenance sub-command

Perform necessary maintenance tasks according to backup configuration:

* merge deltas (into other deltas or full backups)
* delete old revisions

### Restore sub-command

Restore a given backup (most current by default) into the given location (or
back to the source if nothing is specified).

### Scrub sub-command

Check consistency of specified revisions.

### Status sub-command

Show backup inventory and provide summary information about backup health.

## Revision specifications

If a single revision is expected, you can specify full UUIDs,
or numbers. Numbers specify the N-th newest revision (0 being the the newest, 1 the previous revision, and so on).

If multiple revisions may be given you can pass a single revision (as described above) or the world 'all' to match all existing revisions.

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
