========
Overview
========

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


Operations
==========

Full restore
------------

The most important question is: I screwed up -- how do I get my data back?

Here's the fast answer to make a full restore of the most recent backup::

   $ cd /srv/backy/my-virtual-machine
   $ dd if=last of=/srv/kvm/my-virtual-machine.img bs=4096k

If you like to pick a specific version, it's only a little more effort::

   $ cd /srv/backy/my-virtual-machine
   $ backy status
   +---------------------+------------+------------+---------+--------------+
   | Date                | ID         |       Size |   Durat | Tags         |
   +---------------------+------------+------------+---------+--------------+
   | 2015-11-04 11:09:26 | UT7PkENubw |  60.00 GiB | 845.0 s | weekly,daily |
   | 2015-11-05 10:32:03 | fPnbSvEHHy | 264.85 MiB |  88.1 s | daily        |
   | 2015-11-06 10:32:03 | cErS5GJ5sL | 172.34 MiB |  84.5 s | daily        |
   +---------------------+------------+------------+---------+--------------+
   == Summary
   3 revisions
   60.43 GiB data (estimated)
   $ dd if=fPnbSvEHHymfztN9FuegLQ of=/srv/kvm/my-virtual-machine bs=4096k


Restoring individual files
--------------------------

The image files are exact copies of the data from the virtual disks. You can use
regular Linux tools to interact with them::

    $ cd /srv/backy/my-virtual-machine
    $ ls -l last
    lrwxrwxrwx 1 root root  36 Apr 25 10:13 last -> cErS5GJ5sLdsk9L6oCs4ia
    $ kpartx -av cErS5GJ5sLdsk9L6oCs4ia
    add map loop0p1 (253:9): 0 41934815 linear /dev/loop0 8192
    $ mkdir /root/restore
    $ mount -o ro /dev/mapper/loop0p1 /root/restore
    $ cd /root/restore
    $ ls
    bin  boot  dev  etc  home  lib  lost+found  media  mnt  opt  proc  root  run
    sbin  srv  sys  tmp  usr  var

To clean up::

    $ umount /root/restore
    $ kpartx -d cErS5GJ5sLdsk9L6oCs4ia


Creating backups
----------------

Create a configuration file (see man page for details). Spawn the scheduler with
your favourite init system::

  backy scheduler -c /path/to/backy.conf

The scheduler runs in the foreground until it is shot by SIGTERM. On resume, the
scheduler re-runs missed backup jobs to some degree.

Log output goes to `backy.log` in the current directory by default.


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


Authors
=======

* Daniel Kraft <daniel.kraft@d9t.de>
* Christian Theune <ct@flyingcircus.io>
* Christian Kauhaus <kc@flyingcircus.io>


License
=======

GPLv3


Links
=====

* `Bitbucket repository <https://bitbucket.org/flyingcircus/backy>`_
* `PyPI page <https://pypi.python.org/pypi/backy>`_
* `Online docs <http://pythonhosted.org/backy/>`_
* `Build server <https://builds.flyingcircus.io/job/backy/>`_

.. vim: set ft=rst spell spelllang=en:
