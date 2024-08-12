
# Daemon

Backy has a daemon that is responsible to:

1. Schedule jobs in a timely manner according to their configuration.

2. Provide an HTTP API to allow multiple backy servers to interact with each
   other.

There is a (PostgreSQL) database to store metadata about backups that both
the daemon, the CLI (including the sources) interact with.

# CLI

The are two levels of CLI interactions:

1. The main `backy` command provides administrators interaction capabilities
   with the backy environment on a server to retrieve status information,
   run backups, restore data and some maintenance tasks.

2. Backy itself interacts with sources through a second layer of CLI commands,
   specific to each source. They are called by the higher level CLI as well as
   from the daemon. We use this layering to allow implementing sources in
   different languages.

The CLI ideally does not interact with the daemon directly, but by inspecting
or updating the database.

# Nomenclature

Words within the context of backup software are a bit muddy, specifically
the meaning of "a backup". We decided to take inspiration from the git dictionary
and use it the following way:

1. A **repository** is - similar to git - the logical container for the user
   data relevant to one thing that we are backing up.

2. A **source** provides the data that should be backed up. Different kinds
   of sources can model arbitrary data models: backy does not care whether 
   you are backing up virtual disk images or S3 buckets.

3. A **revision** specifies the state of the source at a certain point in time
   and corresponds to what would be colloquially called "a backup".

4. The daemon uses a **job** for every repository to execute the steps necessary
   to perform regular backups with all surrounding management tasks like
   garbage collection, verification, etc.
