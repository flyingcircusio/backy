Backy manual page
=================

Synopsis
--------

**backy** [*GLOBAL OPTIONS*] *ACTION* [*OPTIONS*]

**backy scheduler** [**-c** *CONFIG*]

**backy check** [**-c** *CONFIG*]

**backy** [**-b** *DIRECTORY*] **backup** *TAGS*

**backy** [**-b** *DIRECTORY*] **find** [**-r** *REVISION*]

**backy** [**-b** *DIRECTORY*] **init** *TYPE* *SOURCE*

**backy** [**-b** *DIRECTORY*] **restore** *TARGET*

**backy** [**-b** *DIRECTORY*] **status**

**backy** --help


Description
-----------

**backy** performs backup and recovery for block devices. Backup revisions are
stored as binary images. This allows for trivial recovery.  **backy**'s data
storage relies on COW-capable filesystems like **btrfs** or **ZFS**.

Typically, **backy scheduler** is started with a configuration file describing
all backup jobs. A *job* consists of a *source* specification and a *schedule*
and creates a set of backup *revisions* in a job directory on the backup
server.  The scheduler invokes each job's low-level commands when they are due.
Manually invoked low-level commands do not interfer with those spawned by the
scheduler.

**backy** employs a flexible retention scheme. Each backup is tagged with a set
of free-form tags. Tags are associated with retention times in the configuration
file. When the retention time for a given tag has expired, the tag is marked
invalid. Once all tags associated with a revision are invalidated, the scheduler
removes that revision.


Options
-------

Global options
^^^^^^^^^^^^^^

Global options can be combined with any subcommand.

-l LOGFILE, --logfile LOGFILE
    Writes logs into the specified log file. If this option is not specified,
    logs are written to stdout. It is safe to rename log files for log
    rotation. **backy** will reopen them automatically.

-v, --verbose
    Includes diagnostic messages in log output.

-b DIRECTORY, --backupdir DIRECTORY
    The given directory is used for this backup job. Useful for low-level
    subcommands like **backup**, **find** etc. Although technically permitted,
    this option has no effect with the **scheduler** and **check** subcommands.
    If no **-b** option is given, the current directory is used.

-h, --help
    Prints a usage summary. Can be combined with action subcommands to give
    usage instructions for that specific subcommand.

Action subcommands
^^^^^^^^^^^^^^^^^^

**scheduler**
    Starts a long-running process which reads job definitions from a
    configuration file (see below). Individual backups are started according to
    the schedule defined in the configuration file.  Job schedules are defined
    by relative intervals (for example, run a daily backup every 24h).  The
    exact backup times are determined algorithmically so that load on the backup
    server is evenly distributed.

    Note that the scheduler does not daemonize itself. It is intended to be
    forked beforehand from the init system. If the scheduler is restarted after
    being off for a while, it will automatically catch up on jobs missed in the
    meantime.

**check**
    Returns an overall status if all backup jobs are serviced in time. The
    output of the **check** subcommand is given in a Nagios-compatible format,
    so that it can be easily processed by various monitoring systems.

    A backup job is considered in time if it is no more late than 1.5 times its
    regular interval. For example, a job that is supposed to run every 24h will
    be considered stale after 36h. If any defined job is stale, **backy check**
    will return a CRITICAL state.

**backup** *TAG*, *TAG*, ...
    Creates a new revision of an existing backup job. The job directory
    must be specified using the **-b** option. A comma-separated set of *tags*
    is assigned to the revision for retention control. Note that the tag set
    must be passed as single argument. Use shell quotes if needed.

    Use this subcommand to create a backup manually regardless of schedule.

**find -r** *REVISION*
    Outputs the full path to the image representing the given revision or the
    latest image if no revision is specified. See below for possible revision
    specifications.

**init** *TYPE* *SOURCE*
    Creates a fresh job directory and writes an initial configuration. The job
    directory should be given using the global **-b** option. *TYPE* is the name
    of a source type. The meaning of the *SOURCE* parameter depends on the
    selected source type and is explained in Section :ref:`source_types`.

    Note that this command should hardly be invoked manually. The recommended
    way is to define jobs in the scheduler config file and let **backy
    scheduler** take care of the rest.

**restore** *TARGET*
    Copies out an backup image to a target file or device. Writes data to stdout
    if *TARGET* is **-**. An alternative approach would be to locate the desired
    revision via **backy find** and copy the image file using standard UNIX
    tools to the target location.

**backy status**
    Prints out a table containing details about all revisions present in the job
    directory. Details include a timestamp, the revision ID, image size, backup
    duration, and a set of tags. Note that incomplete revisions are indicated by
    a zero backup duration. They are cleared out at the next backup run.

Subcommand-specific options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**-c** *CONFIG*
    Path to the scheduler configuration file. Defaults to `/etc/backy.conf`. See
    below for an detailed description of the configuration file format.

    Valid for **scheduler** and **check** subcommands.

**-r** *REVISION*
    Selects a revision other than the last revision.

    Revisions can be specified in the following ways:

    * A full revision ID as printed with **backy status**. ID prefixes are OK as
      long as they are unique.
    * A relative revision count: 0 is the last revision, 1 the one before, ...
    * The key word **last** or **latest** as alias for the last revision.
    * A revision tag. If several revisions with the given tag exist, the newest
      one will be given.

    Valid for **find** and **restore** subcommands.

.. _source_types:

Source types
^^^^^^^^^^^^

**backy** comes with a pluggable architecture to support backup sources of
various types.

file
    Reads regular files on the file system, for example a VM image file. Note
    that this source type does not support consistent snapshots. It is advisable
    to freeze the file as long as the backup is running.

    Configuration parameters:

    filename
        Path to the source file.

    Init syntax:

        **backy init file** *FILENAME*

ceph
    Reads Ceph RBD images. For each image, a snapshot is created to ensure
    consistency. The last snapshot is always kept to support efficient deltas on
    subsequent backups. It is, however, safe to remove a Ceph snapshot. If old
    revisions exist but no old snapshot, **backy** falls back to a less
    efficient local delta algorithm. Note that the snapshot should represent a
    consistent file system state to support reliable restore. Thus, **fsfreeze**
    should be invoked inside a running VM directly before a snapshot is
    created.

    Configuration parameters:

    pool
        The Ceph pool to use, for example *rbd*.

    image
        The RBD image name to use.

    Init syntax:

        **backy init ceph-rbd** *POOL*/*IMAGE*

flyingcircus
    Reads Ceph RBD images and provides integration with the hosting
    infrastructure at http://flyingcircus.io. This source type is probably not
    useful for the general public. It accepts all parameters of the **ceph-rbd**
    type.

    Additional configuration parameters:

    vm
        Triggers **fsfreeze** via Consul on the named source VM.

    consul_acl_token (optional)
        Credentials used to talk to the Consul server.

    Init syntax:

        **backy init flyingcircus** *POOL*/*IMAGE*,*VM*,*CONSUL_ACL_TOKEN*

Exit Status
-----------

**backy** generally exists with exit status 0 if the operation completed
successfully, 1 in all other cases. A notable exception is that **backy check**
exists with status 2 to indicate a CRITICAL check outcome.


Environment
-----------

Several environment variable starting with *BACKY_* are used to specify paths to
external utilities. See `exp_deps.py` in the source distribution for details.

The **ceph-rbd** backup source type is influenced by several Ceph-specific
environment variables like **CEPH_CLUSTER** or **CEPH_ARGS**.

**backy scheduler** processes exit cleanly on SIGTERM.


Telnet shell
------------

The schedules opens a telnet server on localhost port 6023 for live
inspection. The telnet interface accepts the following commands:

jobs [REGEX]
    Prints an overview of all configured jobs together with their last and
    next backup run. An optional (extended) regular expression restricts output
    to matching job names.

status
    Dumps internal server status details.

quit
    Exits the telnet shell.


Files
-----

/etc/backy.conf
^^^^^^^^^^^^^^^

The main configuration file is read by the scheduler. It is a hierarchically
structured key/value expression in YAML format.

A description of top-level keys with their sub-keys follows. There is also a
full example configuration in Section :ref:`example` below.

config
    Defines global scheduler options.

    base-dir
        Directory containing all job subdirectories. Defaults to `/srv/backy`.

    worker-limit
        Maximum number of concurrent processes spawned by the scheduler.
        Defaults to 1 (no parallel backups).

    status-file
        Path to a JSON status dump which is regularly updated by the scheduler
        and evaluated by **backy check**. Defaults to `{base-dir}/status`.

.. _schedules:

schedules
    Provides schedule definitions. Each sub-key is a schedule name. A schedule
    consists of a set of run intervals (like daily, weekly, ...) which are
    described by *interval* and *keep* parameters. The overall structure of this
    section is as follows::

        schedules:
            SCHED1:
                INTERVAL1:
                    interval: ...
                    keep: ...
                INTERVAL2:
                    interval: ...
                    keep: ...
                ...
            ...

    Interval parameters are:

    interval
        How often a new revision will be created. Consists of an integer with
        one of the suffixes *s* (seconds), *m* (minutes), *h* (hours), *d*
        (days), *w* (weeks).

    keep
        How many revisions of the given interval will be kept. Must be a
        positive integer.

jobs
    Defines backup jobs and associates them with schedules. Sub-keys name
    individual jobs. The overall structure is as follows::

        jobs:
            JOB1:
                schedule: SCHED
                source:
                    type: TYPE
                    [type-specific options]
            JOB2:
            ...

    Each job must define the following keys:

    schedule
        Name of the schedule (as defined **schedules**) that
        defines how often this job is run.

    source
        Configures the data source for this job. Include a sub-key **type**
        which defines the source type along with other parameters defined by the
        selected source type. See Section :ref:`source_types` for a list
        of source types along with their configuration parameters.

/srv/backy
^^^^^^^^^^

Default data directory which contains subdirectories for all backup jobs,
subject to the **base-dir** global configuration setting.

/srv/backy/{job}
^^^^^^^^^^^^^^^^

Individual backup job directory. Should be passed via the **-b** option
if backy subcommands are invoked manually.  A job directory can be safely
deleted to remove a complete backup job.

/srv/backy/{job}/config
^^^^^^^^^^^^^^^^^^^^^^^

Job-specific configuration snippet configuring a job's backup source. This file
gets written by **backy init** or during the scheduler with information from the
main configuration file. Its purpose is to support stand-alone invocation of
low-level subcommands like **backy backup**.

Notes
-----

The base directory should be placed on a COW-capable filesystem as backing store
to avoid excessive space consumption. The underlying filesystem is expected to
support **cp --reflink**.

**backy** creates new revisions by creating a shallow copy of image representing
the revision before. A delta between the current state of the source and the
state at the time of the last revision is computed and used to update the newest
image. Unmodified portions are kept as shallow copies by the underlying
filesystem. In addition, retention caused random copies in the chain to be
deleted, forcing the filesystem to adjust its references.

Running **backy** continuously causes extent trees of the underlying filesystem
to fragment quite heavily. This may degrade performance. To mitigate this
problem on a btrfs backing store, we suggest to run btrfs maintenance on a
regular basis. A suitable cron job should include::

    btrfs filesystem defragment -r /srv/backy
    btrfs balance start /srv/backy
    btrfs scrub start /srv/backy

It is also advisable to mount the underlying filesystem with data compression
enabled.

.. _example:

Example
-------

A main configuration file containing all source types may look like this:

.. literalinclude:: backy.conf.example

This configuration file defines two schedules: "default" and "frequent". The
"default" schedule creates daily, weekly, and monthly backups with different
retention times. The "frequent" schedule creates hourly, daily, and weekly
backups. Weekly backups are kept for 12 weeks and no monthly backups are created
in that case. Three backup jobs are configured with different source types and
schedules.

After the backup directory has been created on a COW-capable filesystem, the
scheduler can now be started with::

    backy -c /path/to/main.conf -l /path/to/backy.log scheduler

The running scheduler may now be inspected via telnet::

    telnet localhost 6023
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    backy-2.0 % jobs
    +--------+-----+---------+-------------------------+-----------+----------+-------------------------+--------------+
    | Job    | SLA | Status  | Last Backup             | Last Tags | Last Dur | Next Backup             | Next Tags    |
    +--------+-----+---------+-------------------------+-----------+----------+-------------------------+--------------+
    | test01 | OK  | waiting | 2015-11-05 11:56:10 UTC | daily     |   50.7 s | 2015-11-06 11:56:10 UTC | daily,weekly |
    | test02 | OK  | waiting | 2015-11-05 10:32:03 UTC | daily     |   88.1 s | 2015-11-06 10:32:03 UTC | daily        |
    | test03 | OK  | waiting | 2015-11-06 09:49:27 UTC | hourly    |   17.7 s | 2015-11-06 10:15:09 UTC | hourly       |
    +--------+-----+---------+-------------------------+-----------+----------+-------------------------+--------------+

Information about individual revisions can be obtained using **backy status**::

    backy -b /my/backydir/test03 status
    +-------------------------+------------------------+------------+---------+--------------+
    | Date                    | ID                     |       Size |   Durat | Tags         |
    +-------------------------+------------------------+------------+---------+--------------+
    | 2015-11-04 20:09:32 UTC | Q5N5Ng5kFzNFVv6FDDDCHi |  10.00 GiB | 150.9 s | daily,weekly |
    | 2015-11-05 06:15:09 UTC | FqZGAe6iG2hadwJoHdpigW | 149.31 MiB |  24.9 s | daily        |
    | 2015-11-06 06:15:09 UTC | ENpdQfhQVgzoiWwT4KuQqP | 184.99 MiB |  28.7 s | daily        |
    | 2015-11-06 09:49:27 UTC | ojtLPmKfGNhJQbaJYo6a4C |  44.61 MiB |  17.7 s | hourly       |
    +-------------------------+------------------------+------------+---------+--------------+
    == Summary
    4 revisions
    10.37 GiB data (estimated)

Use **backy find** to print out the path to the last daily backup, for example::

    backy -b /my/backydir/test03 find -r daily
    /my/backydir/test03/ENpdQfhQVgzoiWwT4KuQqP

To restore files from a revision, use either **backy restore** to copy out a
whole revision or mount a revision image directly to copy out individual files::

    test03# kpartx -av ENpdQfhQVgzoiWwT4KuQqP
    add map loop0p1 (253:0): 0 20961280 linear /dev/loop0 8192
    add map loop0p2 (253:1): 0 2048 linear /dev/loop0 2048
    test03# mount -o ro /dev/mapper/loop0p1 /mnt/restore
    test03# ls /mnt/restore
    bin   dev  home  lost+found  mnt  proc  run   srv  tmp  var
    boot  etc  lib   media       opt  root  sbin  sys  usr
    test03# cp /mnt/restore/home/ckauhaus/important_file ...
    test03# umount /mnt/restore
    test03# kpartx -d ENpdQfhQVgzoiWwT4KuQqP
    loop deleted : /dev/loop0


See also
--------

:manpage:`btrfs(8)`,
:manpage:`cp(1)`,
:manpage:`fsfreeze(8)`,
:manpage:`regex(7)`

`Ceph <http://ceph.com/>`_

`Consul <https://www.consul.io/>`_

`Open ZFS <http://open-zfs.org/>`_

.. vim: set spell spelllang=en:
