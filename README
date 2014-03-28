## Synopsis

    backy [-h,--help] [--version]
    backy [-v|--verbose] {-b|--backup} srcfile dstfile [rollfile] [diffprefix]
    backy [-v|--verbose] {-c|--clean} [-f|--force] dstfile level [diffprefix]
    backy [-v|--verbose] {-r|--restore} [-f|--force] dstfile level restorefile [diffprefix]
    backy [-v|--verbose] {-s|--scrub} [-n|--checkonly] backupfile [rollfile]
    backy {-l|--ls} dstfile [diffprefix]

## Restore

Restore latest image:

    $ backy -r srcfile 0 dstfile

Restore previous image:

    $ backy -r srcfile 1 dstfile

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

### Backup mode (-b)

    srcfile: The file that should be backed up
    dstfile: This file will be a copy of srcfile
    rollfile: Optional: The rollfile (hints for faster backup). Default: <dstfile>.roll diffprefix: Optional: The prefix for the diff files. Default: <dstfile>.diff

### Clean mode (-c)

    dstfile: The file into which the backup was saved earlier
    level: The number of diffs to keep. All other diffs will be deleted. Minimum level is 1
    diffprefix: Optional: The prefix for the diff files. Default: <dstfile>.diff
    -f|--force: Force cleaning all diffs

### Restore mode (-r)

    dstfile: The file into which the backup was saved earlier
    level: The number of diffs to be applied. 0 means: Restore latest image. 1 means:
        restore most recent diff. 2 means: restore the diff one before the most recent and
        so on.
    restorefile: This file will contain the restored image.
    diffprefix: Optional: The prefix for the diff files. Default: <dstfile>.diff
    -f|--force: Force overwriting the target file

### Scrub mode (-s)

    backupfile: The large backup-file (i.e. not a diff!)
    rollfile: The .roll file that matches the backupfile.
        Default: {backupfile}.roll
    -n|--checkonly: Only check and output changes, don't change any data
        (i.e. don't change the rollfile to fix the next backup)
    Reads the full backupfile and compares it to the rollfile.
    In order to let the next backup-run fix the damaged positions, it writes
    empty lines into the rollfile where the checksum doesn't match.
    The data will still be invalid, but will be fixed on the next backup run.

### List (-l)

    dstfile: The file into which the backup was saved earlier
    Shows you a list of level -> date which you may restore


## Examples

    backy -b /dev/sda /mnt/backup/sda.img
    backy -r /mnt/backup/sda.img 2 /dev/sda
    backy -c 7
    backy -l /mnt/backy/sda.img

## Exit status

    0: Copy and diff creation worked
    1: Error during backup / restore

## Author

Daniel Kraft <daniel.kraft@d9t.de>

## License

GPL

## Hacking

    $ hg clone https://bitbucket.org/ctheune/backy
    $ cd backy
    $ virtualenv --python=python2.7 .
    $ bin/python bootstrap.py
    $ bin/buildout
    $ bin/py.test
