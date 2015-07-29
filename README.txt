=====
Backy
=====

**This is work in progress. Parts of this file are currently fiction.**

Backy is a block-based backup utility for virtual machines (i.e. volume files).

Backy is intended to be:

* space-, time-, and network-efficient
* trivial to restore
* reliable

To achieve this, we rely on:

* using a copy-on-write filesystem (like ZFS or btrfs) as the target
  filesystem to achieve space-efficiency
* using a snapshot-capable main storage for our volumes (e.g.
  Ceph, LVM, ...) that allows easy extraction of changes between snapshots
* leverage proven, existing low-level tools
* keep the code-base small, simple, and well-tested.

Synopsis
========

    backy --help

    backy [-b <backupdir>] init

    backy [-b <backupdir>] backup

    backy [-b <backupdir>] status


Disaster recovery / full restore
================================

The most important question is: I screwed up - how do I get my data back?

Here's the fast answer to make a full restore of the most recent backup::

    $ cd /srv/backy/my-virtual-machine
    $ dd if=latest of=/srv/kvm/my-virtual-machine bs=4048000

If you like to pick a specific version, it's only a little more effort::

    $ cd /srv/backy/my-virtual-machine
    $ backy status
    == Revisions
    2014-04-25 10:07:51 96d8b001-0ffc-4149-8c35-cf003f5638d6    20.00 GiB   252s
    2014-04-25 10:13:20 d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec    24.34 MiB   13s

    == Summary
    2 revisions
    20.02 GiB data (estimated)
    $ dd if=96d8b001-0ffc-4149-8c35-cf003f5638d6 of=/srv/kvm/my-virtual-machine bs=4048000


Restoring individual files
==========================

The image files are exact copies of the data from the virtual disks. You can use
regular Linux tools to interact with them::

    $ cd /srv/backy/my-virtual-machine
    $ ls -lah latest
    lrwxrwxrwx 1 root root  36 Apr 25 10:13 last -> d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec
    $ kpartx -av d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec
    add map loop0p1 (253:9): 0 41934815 linear /dev/loop0 8192
    $ mkdir /root/restore
    $ mount -o ro /dev/mapper/loop0p1 /root/restore
    $ cd /root/restore
    $ ls
    bin  boot  dev  etc  home  lib  lost+found  media  mnt  opt  proc  root  run  sbin  srv  sys  tmp  usr  var

To clean up::

    $ cd /srv/backy/my-virtual-machine
    $ umount /root/restore
    $ kpartx -av d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec

Backup sub-command
------------------

Do a backup.

This includes checking whether a backup is needed, cleaning up from previous
incomplete backups, and removing backups that are no longer needed according to
the schedule.

If no backup is needed, just exit silently.

Status sub-command
------------------

Show backup inventory and provide summary information about backup health.

Revision specifications
-----------------------

If a command expects a single revision, you can specify full UUIDs, or numbers.
Numbers specify the N-th newest revision (0 being the the newest, 1 the
previous revision, and so on).

If multiple revisions may be given you can pass a single revision (as described
above) or the word `all` to match all existing revisions.

Examples
--------

(TBD)


Exit status
===========

*   0: Command worked properly.
*   1: An error occured.


Authors
=======

* Daniel Kraft <daniel.kraft@d9t.de>
* Christian Theune <ct@flyingcircus.io>


License
=======

GPLv3


Hacking
=======

Backy is intended to be compatible with Python 3.3 and 3.4. It is expected to
work properly on Linux and Mac OS X, even though specific backends may not be
avaible on some platforms::

    $ hg clone https://bitbucket.org/ctheune/backy
    $ cd backy
    $ virtualenv --python=python3.4 .
    $ bin/pip install zc.buildout
    $ bin/buildout
    $ bin/py.test
