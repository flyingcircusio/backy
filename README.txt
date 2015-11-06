=====
Backy
=====

Backy is a block-based backup utility for virtual machines (i.e. volume files).

Backy is intended to be:

* space-, time-, and network-efficient
* trivial to restore
* reliable.

To achieve this, we rely on:

* using a copy-on-write filesystem (btrfs, ZFS) as the target filesystem to
  achieve space-efficiency,
* using a snapshot-capable main storage for our volumes (e.g.
  Ceph, LVM, ...) that allows easy extraction of changes between snapshots,
* leverage proven, existing low-level tools,
* keep the code-base small, simple, and well-tested.


Recovery
========

Full restore
------------

The most important question is: I screwed up -- how do I get my data back?

Here's the fast answer to make a full restore of the most recent backup::

    $ cd /srv/backy/my-virtual-machine
    $ dd if=latest of=/srv/kvm/my-virtual-machine bs=4096k

If you like to pick a specific version, it's only a little more effort::

    $ cd /srv/backy/my-virtual-machine
    $ backy status
    == Revisions
    2014-04-25 10:07:51 96d8b001-0ffc-4149-8c35-cf003f5638d6    20.00 GiB   252s
    2014-04-25 10:13:20 d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec    24.34 MiB   13s

    == Summary
    2 revisions
    20.02 GiB data (estimated)
    $ dd if=96d8b001-0ffc-4149-8c35-cf003f5638d6 of=/srv/kvm/my-virtual-machine bs=4096k


Restoring individual files
--------------------------

The image files are exact copies of the data from the virtual disks. You can use
regular Linux tools to interact with them::

    $ cd /srv/backy/my-virtual-machine
    $ ls -lah last
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
    $ kpartx -d d95e4f6c-cfef-48ee-aec2-d7c9e91c1bec


Creating backups
================

Create a configuration file (see below). Spawn the scheduler with your favourite
init system::

  backy scheduler -c /path/to/backy.conf

The scheduler runs in the foreground until it is shot by SIGTERM. On resume, the
scheduler re-runs missed backup jobs to some degree.

Log output goes to `backy.log` in the current directory by default.


Interactive console
===================

Telnet into localhost port 6023 to get a console.

.. XXX TBD


Self-checking
=============


Configuration
=============

Backy's job scheduler maintains a set of revisions (snapshots) for each
configured job according to a schedule. Each revision is marked with a set of
tags. A certain number of revisions is kept for each tag.

Refer to `examples/backy.conf` for a full configuration example. The
configuration file is a YAML document with the following top-level sections:

`global` section
----------------

- `base-dir` specifies the root backup directory. Subdirectories are created for
  each specified backup job.
- `worker-limit` restricts the maximum number of concurrent backy jobs.

`schedules` section
-------------------

Defines one or more schedules which are referenced by backup jobs. For example,
the schedules section ::

  schedules:
      mysched:
          daily:
              interval: 1d
              keep: 9
          weekly:
              interval: 7d
              keep: 5

defines a schedule **mysched**: a revision tagged "daily" is created once
a day and a revision tagged "weekly" is created once a week. If there are more
than `keep` revisions with a specific tag, old revisions are deleted. Note that
a revision may have multiple tags. In this case, it is only deleted if the
retention rules for all tags are met.

`jobs` section
--------------

Defines a set of backup jobs (i.e., block devices to be backed up). For each
job, a `source` and a `schedule` must be specified.

Backup Sources
==============

Backy comes with a number of plug-ins which define block-file like sources:

- `file` extracts data from simple image files living on a regular file system.
- `ceph` pulls data from RBD images using `Ceph`_ features like snapshots. Needs
  environment variables like `CEPH_ARGS` to provide access IDs.
- `flyingcircus` is an extension to the `ceph` source which we use internally on
  the `Flying Circus`_ hosting platform. It uses advanced features like
  `Consul`_ integration.

.. _Ceph: http://www.ceph.com/
.. _Flying Circus: http://flyingcircus.io/
.. _Consul: https://consul.io/

It should be easy to write plug-ins for additional sources.


Low-level commands
==================

Generally speaking, use the scheduler. In case of need, the following low-level
commands can be invoked directly.

backy status
------------

Prints a status summary of the current backup job.

backy check
-----------

Indicates if all configured backup jobs have recent backups. The output is given
in a `Nagios-compatible`_ format.

.. _Nagios-compatible: https://nagios-plugins.org/doc/guidelines.html

backy init
----------

Creates an empty job directory.

backy backup
------------

Unconditionally create a revision (backup) with the given tags.

backy restore
-------------

Creates a new image from a specified backup revision. The revision can be
specified in several ways:

* as unique ID prefix (as seen in the output of `backy status`),
* as tag name to select the last backup with the matching tag,
* as keyword 'last' or 'latest'
* as integer N to select the Nth-newest backup before the last backup.

The restore target should be a file path or '-' for stdout.

backy find
----------

Resolves revision specifications (see above) to full image file pathnames.

For example, to expand the path to the second to last weekly backup::

   $ backy find weekly
   /srv/backy.test/myjob/dZmiafS963sUgvCRhZGZpi


Authors
=======

* Daniel Kraft <daniel.kraft@d9t.de>
* Christian Theune <ct@flyingcircus.io>
* Christian Kauhaus <kc@flyingcircus.io>


License
=======

GPLv3

.. vim: set ft=rst sw=3 sts=3 spell spelllang=en:
