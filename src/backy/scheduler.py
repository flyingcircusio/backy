from backy.backup import Archive
from backy.schedule import Schedule
import argparse
import asyncio
import datetime
import os
import random
import sys
import time
import yaml


# keep a centralized system config,

# - which backups exist
# - where are they located (keep the directories data+config+logs as a
#   bundle for easier relocation)
# - configure sets of schedules
# - schedules do not become part of a backup's local config
# --> but expiry? is expiry part of the scheduler then?
# - running backy will always create a backup with the given tags
# - backy then needs to learn lower-level commands like "expire this"?
# - (expiry really is just deleting the files or updating their tag sets)
# - schedule when to run a backup
# - avoid scheduling later and later and accumulating lateness
# - maximum number of parallel jobs
# - spread jobs but try to keep their base schedule stable
# - rearrange when the global config changes: use a hashing mechanism to
#   select when to run backups for various tags?
# - individual backups loose their schedule?
# - simulation moves over to the scheduler?


original_print = print
def print(str):
    original_print('{}: {}'.format(datetime.datetime.now().isoformat(), str))


class Task(object):

    ideal_start = None
    tags = None

    activated = False

    def __init__(self, job):
        self.job = job
        self.tags = set()

    @property
    def name(self):
        return self.job.name

    # Maybe this should be run in an executor instead?
    @asyncio.coroutine
    def backup(self, future):
        print ("{}: running backup {}, was due at {}".format(
            self.job.name, ', '.join(self.tags), self.ideal_start.isoformat()))
        delay = random.randint(1, 120)

        # Update config
        # XXX this isn't true async
        backup_dir = os.path.join(
            self.job.daemon.config['global']['base-dir'],
            self.name)
        if not os.path.exists(backup_dir):
            # We do not want to create leading directories, only
            # the backup directory itself. If the base directory
            # does not exist then we likely don't have a correctly
            # configured environment.
            os.mkdir(backup_dir)
        with open(os.path.join(backup_dir, 'config'), 'w') as f:
            f.write(yaml.dump(self.job.source))

        # Run backup command
        cmd = "{} -b {} backup {}".format(
            self.job.daemon.backy_cmd,
            backup_dir,
            ','.join(self.tags))
        process = yield from asyncio.create_subprocess_shell(cmd)
        yield from process.communicate()

        # Expire backups
        # XXX this isn't true async
        self.job.archive.scan()
        self.job.schedule.expire(self.job.archive)

        future.set_result(self)


class TaskPool(object):
    """The task pool represents a priority queue that keeps
    a continuously updated set of priorities for the items
    with weights that are relative to each other, not absolute.

    It also ensures that the tasks are continuously worked on.

    """

    def __init__(self, daemon):
        self.daemon = daemon

        # Incoming tasks are used to consolidate requests that are
        # already in the queue.
        self.incoming_tasks = asyncio.Queue()

        # Scheduled tasks are kept in a dictionary and ...
        self.scheduled_tasks = {}
        self.check_task_activation = asyncio.Event()

        # inserted into the due_tasks queue when they reach their deadline.
        self.due_tasks = asyncio.PriorityQueue()

        self.workers = asyncio.Semaphore(2)

    @asyncio.coroutine
    def put(self, task):
        # Insert into a queue first, so that we can retrieve
        # them and order them at the appropriate time.
        yield from self.incoming_tasks.put(task)

    @asyncio.coroutine
    def get(self):
        priority, task = yield from self.due_tasks.get()
        return self.scheduled_tasks.pop(task)

    @asyncio.coroutine
    def process_incoming_queue(self):
        print("Starting to process incoming queue")
        while True:
            task = (yield from self.incoming_tasks.get())
            if task.job.name in self.scheduled_tasks:
                self.scheduled_tasks[task.name].tags.update(task.tags)
                continue

            self.scheduled_tasks[task.name] = task
            self.check_task_activation.set()

    @asyncio.coroutine
    def activate_due_tasks(self):
        while True:
            yield from self.check_task_activation.wait()
            self.check_task_activation.clear()

            future_deadlines = []
            now = datetime.datetime.now()
            for task in self.scheduled_tasks.values():
                if task.activated:
                    continue
                elif task.ideal_start <= now:
                    # The activation switch avoids putting the task
                    # into the queue multiple times while it awaits
                    # a free worker slot.
                    task.activated = True
                    yield from self.due_tasks.put(
                        (task.ideal_start, task.name))
                else:
                    future_deadlines.append(task.ideal_start)

            if future_deadlines:
                next_deadline = time.mktime(min(future_deadlines).timetuple())
                time_to_next_deadline = next_deadline - time.time()
                next_activation = self.daemon.loop.call_later(
                    time_to_next_deadline,
                    lambda: self.check_task_activation.set())

    @asyncio.coroutine
    def run(self):
        asyncio.async(self.process_incoming_queue())
        asyncio.async(self.activate_due_tasks())
        print("Starting to work on queue")
        while True:
            # Ok, lets get a worker slot and a task
            yield from self.workers.acquire()
            task = yield from self.get()
            task_future = asyncio.Future()
            # Now, lets select a job
            asyncio.async(task.backup(task_future))
            task_future.add_done_callback(self.finish_task)

    def finish_task(self, future):
        self.workers.release()
        task = future.result()
        print("{}: finished, releasing worker.".format(task.name))


class Job(object):

    name = None
    source = None
    schedule_name = None
    spread = 0

    _generator_handle = None

    def __init__(self, daemon, name):
        self.daemon = daemon
        self.name = name
        self.handle = None

    def configure(self, config):
        self.source = config['source']
        self.schedule_name = config['schedule']
        self.spread = config.get('spread', self.spread)
        self.path = self.daemon.base_dir + '/' + self.name
        self.archive = Archive(self.path)
        self.daemon.loop.call_soon(self.start)

    @property
    def schedule(self):
        return self.daemon.schedules[self.schedule_name]

    @asyncio.coroutine
    def generate_tasks(self):
        """Generates tasks based on the ideal next time in the future.

        This may repetetively submit a task until its time has come and then
        generate other tasks after the ideal next time has switched over.

        It doesn't care whether the tasks have been successfully worked on or
        not. The task pool needs to deal with that.
        """
        while True:
            relative = time.time()
            next_time, next_tags = self.schedule.next(relative, self.spread)

            task = Task(self)
            task.ideal_start = next_time
            task.tags.update(next_tags)
            print("{}: submitting task for {}".format(self.name, next_time))
            yield from self.daemon.taskpool.put(task)

            until_next_interval = (
                time.mktime(next_time.timetuple()) - time.time() + 10)
            yield from asyncio.sleep(until_next_interval)

    def start(self):
        self.stop()
        self._generator_handle = asyncio.async(self.generate_tasks())

    def stop(self):
        if self._generator_handle:
            self._generator_handle.cancel()
            self._generator_handle = None


class BackyDaemon(object):

    worker_limit = 1
    base_dir = '/srv/backy'

    def __init__(self, loop, config_file):
        self.backy_cmd = os.path.join(
            os.getcwd(), os.path.dirname(sys.argv[0]), 'backy')
        self.loop = loop
        self.config_file = config_file
        self.config = None
        self.schedules = {}
        self.jobs = {}
        self.taskpool = TaskPool(self)

    def configure(self):
        # XXX Signal handling to support reload
        if not os.path.exists(self.config_file):
            print('Could not load configuration. '
                  '`{}` does not exist.'.format(self.config_file))
            return
        with open(self.config_file, 'r', encoding='utf-8') as f:
            config = yaml.load(f)
        if config != self.config:
            print ("Config changed - reloading.")
            self.config = config
            self.configure_global()
            self.configure_schedules()
            self.configure_jobs()

    def configure_global(self):
        c = self.config.get('global')
        if not c:
            return
        self.worker_limit = c.get('worker-limit', self.worker_limit)
        print("New worker limit: {}".format(self.worker_limit))
        self.base_dir = c.get('base-dir', self.base_dir)
        print("New backup location: {}".format(self.base_dir))

    def configure_schedules(self):
        # TODO: Ensure that we do not accidentally remove schedules that
        # are still in use? Or simply warn in this case?
        # Use monitoring for that?
        new = {}
        for name, config in self.config['schedules'].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new
        print("Available schedules: {}".format(', '.join(self.schedules)))

    def configure_jobs(self):
        new = {}
        for name, config in self.config['jobs'].items():
            if name in self.jobs:
                job = self.jobs[name]
            else:
                job = Job(self, name)
            new[name] = job
            new[name].configure(config)
        # Stop old jobs.
        for job in self.jobs.values():
            if job not in new:
                job.stop()
        # (Re-)start existing and new jobs.
        for job in new.values():
            job.start()
        self.jobs = new
        print("Configured jobs: {}".format(len(self.jobs)))

    def start(self):
        self.loop.call_soon(self.configure)
        asyncio.async(self.taskpool.run())


def main():
    parser = argparse.ArgumentParser(
        description='Daemon that handles scheduling of backy jobs.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-c', '--config', default='/etc/backy.conf')

    import logging
    logging.basicConfig(level=logging.DEBUG)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(loop, args.config)

    daemon.start()
    # Blocking call interrupted by loop.stop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
