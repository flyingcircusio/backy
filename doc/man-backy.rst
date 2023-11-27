Backy manual page
=================

Synopsis
--------

**backy** [*GLOBAL OPTIONS*] *ACTION* [*OPTIONS*]

**backy** [*GLOBAL OPTIONS*] client [*CLIENT OPTIONS*] *CLIENT ACTION* [*OPTIONS*]

**backy** --help

Scheduler
^^^^^^^^^

**backy scheduler** [**-c** *CONFIG*]

Client
^^^^^^

**backy client check**

**backy client jobs** [*FILTER*]

**backy client status**

**backy client run** *JOB*

**backy client runall**

**backy client reload**

Job
^^^

**backy backup** [**-f**] *TAGS*

**backy restore** [**-r** *REVISION*] *TARGET*

**backy purge**

**backy status** [**--yaml**]

**backy upgrade**

**backy scheduler** [**-c** *CONFIG*]

**backy distrust** [**-r** *REVISION* | [**-f** *FROM*] [**-u** *UNTIL*]]

**backy verify** [**-r** *REVISION*]

**backy forget** **-r** *REVISION*

**backy tags** [**-f**] [**--autoremove**] [**--expect** *TAGS*] {**set**, **add**, **remove**} *REVISION* *TAGS*

**backy expire**


Description
-----------

**backy** performs backup and recovery for block devices. Backup revisions are
stored as deduplicated and compressed chunks.

Typically, **backy scheduler** is started with a configuration file describing
all backup jobs. A *job* consists of a *source* specification and a *schedule*
and creates a set of backup *revisions* in a job directory on the backup
server.  The scheduler invokes each job's low-level commands when they are due.
Manually invoked low-level commands do not interfere with those spawned by the
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
    Writes verbose logs into the specified log file. If this option is not
    specified, logs are written to `/var/log/backy.log` for the **scheduler**
    subcommand. **client** has no default logfile, all other subcommands default
    to `$backupdir/backy.log`.
    Logs will always be written to stderr.

-v, --verbose
    Includes diagnostic messages in stderr log output.

-b DIRECTORY, --backupdir DIRECTORY
    The given directory is used for this backup job. Useful for low-level
    subcommands like **backup**, **status** etc. Although technically permitted,
    this option has no effect with the **scheduler** and **client** subcommands.
    If no **-b** option is given, the current directory is used.

-t TASKID, --taskid TASKID
    ID to include in log output. Useful to identify multiple commands as
    belonging to the same task.
    This value is transmitted to remote servers on contact.

-h, --help
    Prints a usage summary. Can be combined with action subcommands to give
    usage instructions for that specific subcommand.

Client options
^^^^^^^^^^^^^^

**-c** *CONFIG*, **--config** *CONFIG*
    Path to the scheduler configuration file. Defaults to `/etc/backy.conf`. See
    below for an detailed description of the configuration file format.

    Also valid for the *scheduler* subcommand.

**-p** *PEER*, **--peer** *PEER*
    The peer name to use when connecting to the backy daemon.
    Defaults to the cli default configured in the config file.

**--url** *URL*
    The url to use when connecting to the backy daemon.
    Only useful in combination with **--token**.
    Overwrites **--config** and **--peer**.

**--token** *TOKEN*
    The token to use when connecting to the backy daemon.
    Only useful in combination with **--url**.
    Overwrites **--config** and **--peer**.

Action subcommands
^^^^^^^^^^^^^^^^^^

**scheduler** [**-c** *CONFIG*]
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

**client check**
    Returns an overall status if all backup jobs are serviced in time. The
    exit code of the **check** subcommand is given in a Nagios-compatible
    format, so that it can be easily processed by various monitoring systems.

    A backup job is considered in time if it is no more late than 1.5 times its
    regular interval. For example, a job that is supposed to run every 24h will
    be considered stale after 36h. If any defined job is stale, **backy check**
    will return a CRITICAL state.

**client jobs** [*FILTER*]
    Prints out a table about every backup job (including backup directories
    which no longer have a job). Provide a regular expression to filter the
    table. See :ref:`example` section for more details.

**client status**
    Prints out the number of backup jobs for each state. Valid states are:
    Dead, waiting for deadline, waiting for worker slot (*SPEED*),
    running (*SPEED*), failed and finished. *SPEED* can be slow or fast.

**client run** *JOB*
    Triggers an immediate run for the specified job.

**client runall**
    Triggers an immediate run for all jobs.

**client reload**
    Triggers a daemon config reload.

**backup** [**-f**] *TAG*, *TAG*, ...
    Creates a new revision of an existing backup job. The job directory
    must be specified using the **-b** option.

    A comma-separated set of *tags* is assigned to the revision for retention
    control. See the **-f** option for more details.
    Note that the tag set must be passed as single argument. Use shell quotes
    if needed.

    Use this subcommand to create a backup manually regardless of schedule.

**restore** [**-r** REVISION] *TARGET*
    Copies out an backup image to a target file or device. Writes data to stdout
    if *TARGET* is **-**. Use **-r** to specify which revision to restore
    instead of the latest.

**purge**
    Removes all chunks which are no longer referred to by a revision.

**status** [**--yaml**]
    Prints out a table containing details about all revisions present in the job
    directory. Details include a timestamp, the revision ID, image size, backup
    duration, a set of tags, trust state, and the server the revision is
    located on. Note that incomplete revisions are indicated by
    a zero backup duration. They are cleared out at the next backup run.

    Use the **--yaml** flag to get this data in YAML format.

**upgrade**
    Upgrade this backup (incl. its data) to the newest supported version.
    This can take a long time and is intended to be interruptable.

**distrust** [**-r** *REVISION* | [**-f** *FROM*] [**-u** *UNTIL*]]
    Distrust the revision specified by **-r** or all revisions in the range
    from **-f** to **-u**. Defaults to all revisions. See :ref:`trust` for more
    details.

**verify** [**-r** *REVISION*]
    Verifies the revision specified by **-r** or all `DISTRUSTED` revisions if
    unspecified. Revisions which fail verification will be removed. See
    :ref:`trust` for more details.

**forget** **-r** *REVISION*
    Removes the revision specified by **-r**. Unreferenced chunks will be
    removed by the next **purge** call.

**tags** [**-f**] [**--autoremove**] [**--expect** *TAGS*] {**set**, **add**, **remove**} *REVISION* *TAGS*
    Sets, adds, or removes the comma-separated set of tags to the specified
    revision. See the **-f** option for more details.
    Note that the tag set must be passed as single argument. Use shell quotes
    if needed.

    Use **--autoremove** to remove the revision if it has no tags at the end of
    the operation. Otherwise the revision will be removed by the next
    **expire** invocation.

    Use **--expect** to only modify the tags if the existing tags equal the
    specified tags.

**expire**
    Removes tags from revisions which are no longer needed according to the
    specified schedule. Tags with the `manual:` prefix will not be removed.
    Revisions without tags will be removed.

Shared subcommand-specific options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**-r** *REVISION*, **--revision** *REVISION*
    Selects a revision other than the last revision.

    Revisions can be specified in the following ways:

    * A full revision ID as printed with **backy status**.
    * A relative revision count: 0 is the last revision, 1 the one before, ...
    * The key word **last** or **latest** as alias for the last revision.
    * A revision tag. If several revisions with the given tag exist, the newest
      one will be given.

    Valid for **distrust**, **verify**, **forget**, **tags** and **restore**
    subcommands.

**-f**, **--force**
    Disable tag validation. This can be used to assign tags which are not part
    of the schedule and are not starting with `manual:` to a revision.

    Note that the next **expire** call may remove these tags again.

    Valid for **backup** and **tags** subcommands.

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

    cow
        Allow backups in a cow friendly way by only writing changes to the
        target. This attribute is similar to the inverse of full-always in ceph.
        There is no need to change this if you are using the chunked backend.
        Defaults to True.

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

    full-always
        Never create diffs but do always a full backup. No long-lived snapshots
        are needed in this case. This option is meant for volumes with very high
        change rates. Defaults to False.

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

.. _trust:

Trust
-----
Every revision has a trust state of either `TRUSTED`, `VERIFIED` or
`DISTRUSTED`.
After creation a revision has the `TRUSTED` state. Trust state can be
modified via the **verify** and **distrust** commands.

The `VERIFIED` state means that the integrity of this revision has (at some
point in the past) been checked.

The `DISTRUSTED` state means that is possible that the revision has
integrity errors. The existence of such a revision will cause **backy** to
overwrite existing chunks and disable the differential backup implementation.
On each **backup** the latest `DISTRUSTED` revision is verified. A revision
which fails verification is removed.

Some events such as an integrity error on reading a chunk may cause all
revisions to become `DISTRUSTED`.

Exit Status
-----------

**backy** generally exists with exit status 0 if the operation completed
successfully, 1 in all other cases. A notable exception is that **backy
client check** exists with status 2 to indicate a CRITICAL check outcome.


Environment
-----------

Several environment variable starting with *BACKY_* are used to specify paths to
external utilities. See `exp_deps.py` in the source distribution for details.

The **ceph-rbd** backup source type is influenced by several Ceph-specific
environment variables like **CEPH_CLUSTER** or **CEPH_ARGS**.

**backy scheduler** processes exit cleanly on SIGTERM.


Files
-----

/etc/backy.conf
^^^^^^^^^^^^^^^

The main configuration file is read by the scheduler. It is a hierarchically
structured key/value expression in YAML format.

A description of top-level keys with their sub-keys follows. There is also a
full example configuration in Section :ref:`example` below.

global
    Defines global scheduler options.

    base-dir
        Directory containing all job subdirectories.

    worker-limit
        Maximum number of concurrent processes spawned by the scheduler.
        Defaults to 1 (no parallel backups).

    backup-completed-callback
        Command/Script to invoke after the scheduler successfully completed a
        backup. The first argument is the job name. The output of
        `backy status --yaml` is available on stdin.

api
    addrs
        Comma-separated list of listen addresses for the api server
        (default: 127.0.0.1, ::1).

    port
        Port number of the api server (default: 6023).

    tokens
        A Token->Server-name mapping. Used for authenticating incoming api
        requests.

    cli-default
        token
            Default Token to use when issuing api requests via the
            `backy client` command.

peers
    List of known backy servers with url and token. Currently used for
    synchronizing available revisions. The structure is as follows::

        peers:
            SERVER1:
                url: ...
                token: ...
            ...

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


$base-dir/{job}
^^^^^^^^^^^^^^^

Individual backup job directory. Should be passed via the **-b** option
if backy subcommands are invoked manually.  A job directory can be safely
deleted to remove a complete backup job.

$base-dir/{job}/config
^^^^^^^^^^^^^^^^^^^^^^

Job-specific configuration snippet configuring a job's backup source. This file
gets written by the scheduler with information from the
main configuration file. Its purpose is to support stand-alone invocation of
low-level subcommands like **backy backup**.

$base-dir/{job}/quarantine
^^^^^^^^^^^^^^^^^^^^^^^^^^

Contains reports about chunks and the chunks themselves which did not match the
source immediately after revision creation.

Notes when using the legacy cowfile backend
-------------------------------------------

The following notes only apply if you are using the legacy cowfile backend.
New backup jobs will default to the new chunked backend. You can migrate
existing jobs to the new backend with the **upgrade** command.

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

After the backup directory has been created, the
scheduler can now be started with::

    backy -c /path/to/main.conf -l /path/to/backy.log scheduler

The running scheduler may now be inspected via **client** commands::

    backy client -c backy.conf jobs
    ┏━━━━━━━━┳━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
    ┃        ┃     ┃             ┃                      ┃ Last Backup         ┃           ┃               ┃ Next Backup          ┃              ┃
    ┃ Job    ┃ SLA ┃ SLA overdue ┃ Status               ┃ (Europe/Berlin)     ┃ Last Tags ┃ Last Duration ┃ (Europe/Berlin)      ┃ Next Tags    ┃
    ┡━━━━━━━━╇━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
    │ test01 │ OK  │ -           │ waiting for deadline │ 2015-11-05 11:56:10 │ daily     │    50 seconds │ 2015-11-06 11:56:10  │ daily,weekly │
    │ test02 │ OK  │ -           │ waiting for deadline │ 2015-11-05 10:32:03 │ daily     │      a minute │ 2015-11-06 10:32:03  │ daily        │
    │ test03 │ OK  │ -           │ waiting for deadline │ 2015-11-06 09:49:27 │ hourly    │    17 seconds │ 2015-11-06 10:15:09  │ hourly       │
    └────────┴─────┴─────────────┴──────────────────────┴─────────────────────┴───────────┴───────────────┴──────────────────────┴──────────────┘
    3 jobs shown

Information about individual revisions can be obtained using **backy status**::

    backy -b /my/backydir/test03 status
    ┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
    ┃ Date (Europe/Berlin) ┃ ID                     ┃      Size ┃   Duration ┃ Tags         ┃ Trust   ┃ Server ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
    │ 2015-11-04 20:09:32  │ 46mWHmKy2XP3TQwrjbnd9F │  10.0 GiB │  2 minutes │ weekly,daily │ trusted │        │
    │ 2015-11-05 06:15:09  │ ECHtbbhrDUFaenAyZmgHSF │ 149.3 MiB │ 24 seconds │ daily        │ trusted │        │
    │ 2015-11-06 06:15:09  │ NuAHAwfjiSJceoftU3gNbd │ 185.0 MiB │ 28 seconds │ daily        │ trusted │        │
    │ 2015-11-06 09:49:27  │ BxG4RUvCfn2oDyKkmdjkPa │  44.6 MiB │ 17 seconds │ hourly       │ trusted │        │
    └──────────────────────┴────────────────────────┴───────────┴────────────┴──────────────┴─────────┴────────┘
    4 revisions containing 10.4 GiB data (estimated)


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
