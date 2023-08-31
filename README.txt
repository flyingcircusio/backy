========
Overview
========

Backy is a block-based backup and restore utility for virtual machine images.

Backy is intended to be:

* space-, time-, and network-efficient
* trivial to restore
* reliable.

To achieve this, we rely on:

* space-efficient storages (CoW filesystems, content-hashed chunking)
* using a snapshot-capable source for our volumes (i.e. Ceph RBD)
  that allows easy extraction of changes between snapshots,
* leverage proven, existing low-level tools,
* keep the code-base small, simple, and well-tested.

We also have a few ground rules for the implementation:

* VM data is stored self-contained on the filesystem and can be
  moved between servers using regular FS tools like copy, rsync, or such.

* No third party daemons are required to interact with backy: no database
  server. The scheduler daemon is only responsible for scheduling and simply
  calls regular CLI commands to perform a backup. Backy may interact with
  external daemons like Ceph or Consul, depending on the source storage
  implementation.


Operations
==========

Full restore
------------

Check which revision to restore::

  $ backy -b /srv/backy/<vm> status

Maybe set up the Ceph environment - depending on your configuration::

  $ export CEPH_ARGS="--id $HOSTNAME"

Restore the full image through a Pipe::

  $ backy restore -r <revision> - | rbd import - <pool>/<rootimage>


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


Performance
===========

Backy is designed to use all of the available storage and network bandwidth by
running several instances in parallel. The backing storage must be prepared for
this kind of (mixed) load. Finding optimal settings needs a bit of experimentation
given that hardware and load profiles differ from site to site. The following
section contains a few points to start off.

Storage backend
---------------

If the backing storage is a RAID array, its stripe size should be aligned with
the filesystem. We have made good experiences with 256k stripes. Also check for
512B/4K block misalignments on HDDs. We're using it usually with RAID-6 and have
seen reasonable performance with both hardware and software RAID.

Filesystem
----------

We generally recommend XFS since it provides a high degree of parallelism and is
able to handle very large directories well.

Note that the standard `cfq` I/O scheduler is not a good pick for
highly parallel bulk I/O on multiple drives. Use `deadline` or `noop`.

Kernel
------

Since backy performs a lot of metadata operations, make sure that inodes and
dentries are not evicted from the VFS cache too early. We found that lowering
the `vm.vfs_cache_pressure` sysctl can make quite a difference in total backup
performance. We're currently getting good results setting it to `10`.
You may also want to increase `vm.min_free_kbytes` to avoid page allocation
errors on 10 GbE network interfaces.

Development
===========

Backy has switched to using `poetry` to manage its dependencies. This means
that you can use `poetry install` to install dependencies from PyPI.

If you don't have `backy` in your PATH when developing, enter the poetry
virtualenv with `poetry shell` or if you're using nix with `nix develop`.

You can build backy with `poetry build` making a wheel and a tar archive
in the `dist` directory, or by running `nix build`.

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
