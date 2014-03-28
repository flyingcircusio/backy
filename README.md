## Synopsis

    backy [-h,--help] [--version]
    backy backup srcfile backupfile
    backy clean [-r|--revision <revision>] [-f|--force] backupfile
    backy restore [-f|--force] [-r|--revision <revision>] backupfile restorefile
    backy scrub [-n|--checkonly] backupfile
    backy ls dstfile

## Restore

Restore latest image:

    $ backy restore backupfile restorefile

Restore previous image:

    $ backy restore -r 1 backupfile restorefile

## Description

Backy copies your large file to the destination without flooding your buffers
and caches and it tries to do this with the least possible impact to the
system's performance.

It creates a rollfile which contains information about the source- file so that
backy knows which parts to copy to dstfile when run a second time.

It also creates diffs to the current version so that an earlier version can be
recreated. The diffs only contain changed data, gzipped.

The diffs will be stored to diffprefix.<number> where backy increments the
number by itself.

### Backup sub-command

    srcfile: The file that should be backed up
    backupfile: This file will be a copy of srcfile

### Clean sub-command

    backupfile: The file into which the backup was saved earlier
    --revision: The revision to start cleaning.
    -f|--force: Force cleaning all diffs. If not set at least one differential revision must remain.

### Restore sub-command

    backupfile: The file into which the backup was saved earlier
    --revision: The number of diffs to be applied. 0 means: Restore latest image. 1 means:
        restore most recent diff. 2 means: restore the diff one before the most recent and
        so on.
    restorefile: This file will contain the restored image.
    -f|--force: Force overwriting the target file if it exists.

### Scrub sub-command

    backupfile: The large backup-file (i.e. not a diff!)
    -n|--checkonly: Only check and output changes, don't change any data
        (i.e. don't change the rollfile to fix the next backup)
    Reads the full backupfile and compares it to the rollfile.
    In order to let the next backup-run fix the damaged positions, it writes
    empty lines into the rollfile where the checksum doesn't match.
    The data will still be invalid for the time being, but will be updated on the next backup run.

### List sub-command

    backupfile: The file into which the backup was saved earlier
    Shows you a list of level -> date which you may restore


## Examples

    backy backup /dev/sda /mnt/backup/sda.img
    backy restore -f -r 2 /mnt/backup/sda.img /dev/sda
    backy clean -r 7
    backy ls /mnt/backy/sda.img

## Exit status

    0: Copy and diff creation worked
    1: Error during backup / restore

## Authors

Daniel Kraft <daniel.kraft@d9t.de>
Christian Theune <ct@gocept.com>

## License

GPL

## Hacking

    $ hg clone https://bitbucket.org/ctheune/backy
    $ cd backy
    $ virtualenv --python=python2.7 .
    $ bin/python bootstrap.py
    $ bin/buildout
    $ bin/py.test
