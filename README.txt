========
Overview
========

Backy is a block-based backup and restore utility for virtual machine images.

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


Operations
==========

Full restore
------------

The most important question is: I screwed up -- how do I get my data back?

Here's the fast answer to make a full restore of the most recent backup::

   $ cd /srv/backy/my-virtual-machine
   $ dd if=last of=/srv/kvm/my-virtual-machine.img bs=4096k

In case Ceph is used for image storage, just import the image directly::

   $ rbd import last rbd/my-volume

If you like to pick a specific version, it's only a little more effort::

   $ backy status
   +---------------------+------------+------------+---------+--------------+
   | Date                | ID         |       Size |   Durat | Tags         |
   +---------------------+------------+------------+---------+--------------+
   | 2015-11-04 11:09:26 | UT7PkENubw |  60.00 GiB | 845.0 s | weekly,daily |
   | 2015-11-05 10:32:03 | fPnbSvEHHy | 264.85 MiB |  88.1 s | daily        |
   | 2015-11-06 10:32:03 | cErS5GJ5sL | 172.34 MiB |  84.5 s | daily        |
   +---------------------+------------+------------+---------+--------------+
   3 revisions containing 60.43 GiB data (estimated)
   $ dd if=fPnbSvEHHymfztN9FuegLQ of=/srv/kvm/my-virtual-machine bs=4096k

Or try `backy find` to locate the latest backup for a specific tag::

   $ rbd import $(backy find -r weekly) rbd/my-volume


Restoring individual files
--------------------------

The image files are exact copies of the data from the virtual disks. You can use
regular Linux tools to interact with them::

    $ kpartx -av last
    add map loop0p1 (253:9): 0 41934815 linear /dev/loop0 8192
    $ mkdir /mnt/restore
    $ mount -o ro /dev/mapper/loop0p1 /mnt/restore
    $ cd /mnt/restore
    $ ls
    bin  boot  dev  etc  home  lib  lost+found  media  mnt  opt  proc  root  run
    sbin  srv  sys  tmp  usr  var

To clean up::

    $ umount /mnt/restore
    $ kpartx -d last


Setting up backy
----------------

#. Create a sufficiently large backup partition using a COW-capable filesystem
   like btrfs and mount it under `/srv/backy`.

#. Create a configuration file at `/etc/backy.conf`. See man page for details.

#. Start the scheduler with your favourite init system::

      backy -l /var/log/backy.log scheduler -c /path/to/backy.conf

   The scheduler runs in the foreground until it is shot by SIGTERM.

#. Set up monitoring using `backy check`.

#. Set up log rotation for `/var/log/backy.conf` and `/srv/backy/*/backy.log`.

The file paths given above match the built-in defaults, but paths are fully
configurable.


Features
========

Telnet shell
------------

Telnet into localhost port 6023 to get an interactive console. The console can
currently be used to inspect the scheduler's live status.


Self-check
----------

Backy includes a self-checking facility. Invoke `backy check` to see if there is
a recent revision present for all configured backup jobs::

   $ backy check
   OK: 9 jobs within SLA

Both output and exit code are suited for processing with Nagios-compatible
monitoring systems.


Pluggable backup sources
------------------------

Backy comes with a number of plug-ins which define block-file like sources:

- **file** extracts data from simple image files living on a regular file
  system.
- **ceph-rbd** pulls data from RBD images using Ceph features like snapshots.
- **flyingcircus** is an extension to the `ceph-rbd` source which we use
  internally on the `Flying Circus`_ hosting platform. It uses advanced features
  like Consul integration.

.. _Flying Circus: http://flyingcircus.io/

It should be easy to write plug-ins for additional sources.


Adaptive verification
---------------------

Backy always verifies freshly created backups. Verification scale depends on
the source type: file-based sources get fully verified. Ceph-based sources are
verified based on random samples for runtime reasons.

Zero-configuration scheduling
-----------------------------

The backy scheduler is intended to run continuously. It will spread jobs
according to the configured run intervals over the day. After resuming from an
interruption, it will reschedule missed jobs so that SLAs are still kept if
possible.

Backup jobs can be triggered at specific times as well: just invoke `backy
backup` manually.


Authors
=======

* Christian Theune <ct@flyingcircus.io>
* Christian Kauhaus <kc@flyingcircus.io>
* Daniel Kraft <daniel.kraft@d9t.de>


License
=======

GPLv3


Links
=====

* `Bitbucket repository <https://bitbucket.org/flyingcircus/backy>`_
* `PyPI page <https://pypi.python.org/pypi/backy>`_
* `Online docs <http://pythonhosted.org/backy/>`_
* `Build server <https://builds.flyingcircus.io/job/backy/>`_

.. vim: set ft=rst spell spelllang=en sw=3:
