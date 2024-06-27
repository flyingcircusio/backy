import datetime
import fcntl
import re
from enum import Enum
from math import ceil, floor
from pathlib import Path
from typing import IO, List, Literal, Optional, TypedDict, Any

import tzlocal
import yaml
from structlog.stdlib import BoundLogger

import backy.source
from backy.utils import (
    duplicates,
    list_get,
    list_rindex,
    list_split,
    min_date,
    unique,
)

from .revision import Revision, Trust, filter_schedule_tags
from .schedule import Schedule


class RepositoryNotEmpty(RuntimeError):
    pass


class StatusDict(TypedDict):
    job: str
    sla: str
    sla_overdue: int
    status: str
    last_time: Optional[datetime.datetime]
    last_tags: Optional[str]
    last_duration: Optional[float]
    next_time: Optional[datetime.datetime]
    next_tags: Optional[str]
    manual_tags: str
    problem_reports: List[str]
    unsynced_revs: int
    local_revs: int


class Repository(object):
    """A repository stores and manages backups for a single source.

    The repository handles metadata information around backups, manages the
    schedule and tags and can expire revisions.

    A single backup for something (an RBD disk image, an S3 pool of
    buckets, ...) is called a revision and thus we use "backup" synomymously
    with "revision".

    The actual implementation of making and restoring backups as well as
    storing the data is provided by the `source` implementations.

    """

    path: Path
    schedule: Schedule
    history: list[Revision]
    log: BoundLogger

    _by_uuid: dict[str, Revision]
    _lock_fds: dict[str, IO]
    sourcetype: type[backy.source.Source]

    def __init__(
        self,
        path: Path,
        source: backy.source.Source,
        schedule: Schedule,
        log: BoundLogger,
    ):
        self.schedule = schedule
        self.source = source
        self.source.bind(self, log)
        self.log = log.bind(subsystem="backup")
        self.path = path.resolve()
        self._lock_fds = {}

    def connect(self):
        self.path.mkdir(parents=True, exist_ok=True)
        self.scan()

    @staticmethod
    def from_config(config: dict[str, Any], log: BoundLogger) -> "Repository":
        schedule = Schedule()
        schedule.configure(config["schedule"])

        source = backy.source.factory_by_type(
            config["source"]["type"]
        ).from_config(config["source"])

        return Repository(config['path'], source, schedule, log)

    @property
    def problem_reports(self) -> list[str]:
        return []

    # I placed this on the class because this is usually used in conjunction
    # with the class and improves cohesiveness and readability IMHO.
    @staticmethod
    def locked(target=None, mode=None):
        if mode == "shared":
            mode = fcntl.LOCK_SH
        elif mode == "exclusive":
            mode = fcntl.LOCK_EX | fcntl.LOCK_NB
        else:
            raise ValueError("Unknown lock mode '{}'".format(mode))

        def wrap(f):
            def locked_function(self, *args, skip_lock=False, **kw):
                if skip_lock:
                    return f(self, *args, **kw)
                if target in self._lock_fds:
                    raise RuntimeError("Bug: Locking is not re-entrant.")
                target_path = self.path / target
                if not target_path.exists():
                    target_path.touch()
                self._lock_fds[target] = target_path.open()
                try:
                    fcntl.flock(self._lock_fds[target], mode)
                except BlockingIOError:
                    self.log.warning(
                        "lock-no-exclusive",
                        _fmt_msg="Failed to get exclusive lock for '{function}'.",
                        function=f.__name__,
                    )
                    raise
                else:
                    try:
                        return f(self, *args, **kw)
                    finally:
                        fcntl.flock(self._lock_fds[target], fcntl.LOCK_UN)
                finally:
                    self._lock_fds[target].close()
                    del self._lock_fds[target]

            locked_function.__name__ = "locked({}, {})".format(
                f.__name__, target
            )
            return locked_function

        return wrap

    @property
    def name(self) -> str:
        return self.path.name

    def to_dict(self):
        return self.config

    def scan(self) -> None:
        self.history = []
        self._by_uuid = {}
        for f in self.path.glob("*.rev"):
            if f.is_symlink():
                # Ignore links that are used to create readable pointers
                continue
            r = Revision.load(f, self, self.log)
            if r.uuid not in self._by_uuid:
                self._by_uuid[r.uuid] = r
                self.history.append(r)
        # The history is stored: oldest first. newest last.
        self.history.sort(key=lambda r: r.timestamp)

    def touch(self):
        self.path.touch()

    def set_purge_pending(self):
        self.path.joinpath(".purge_pending").touch()

    def clear_purge_pending(self):
        self.path.joinpath(".purge_pending").unlink(missing_ok=True)

    def get_history(
        self, *, clean: bool = False, local: bool = False
    ) -> list[Revision]:
        return [
            rev
            for rev in self.history
            if (not clean or "duration" in rev.stats)
            and (not local or not rev.server)
        ]

    @property
    def clean_history(self) -> List[Revision]:
        """History without incomplete revisions."""
        return self.get_history(clean=True)

    @property
    def local_history(self):
        """History without incomplete revisions."""
        return self.get_history(local=True)

    @property
    def contains_distrusted(self) -> bool:
        return any(
            (
                r == Trust.DISTRUSTED
                for r in self.get_history(clean=True, local=True)
            )
        )

    def validate_tags(self, tags):
        missing_tags = (
            filter_schedule_tags(tags) - self.schedule.schedule.keys()
        )
        if missing_tags:
            self.log.error(
                "unknown-tags",
                _fmt_msg="The following tags are missing from the schedule: "
                "{unknown_tags}\n"
                "Check the config file, add the `manual:` prefix or disable "
                "tag validation (-f)",
                unknown_tags=", ".join(missing_tags),
            )
            raise RuntimeError("Unknown tags")

    def prevent_remote_rev(self, revs: Optional[List[Revision]] = None):
        revs = revs if revs is not None else self.history
        remote = [r for r in revs if r.server]
        if remote:
            self.log.error(
                "remote-revs-disallowed",
                _fmt_msg="Can not modify trust state of remote revisions "
                "locally.\n"
                "Either include a filter to exclude them (local)\n"
                "or edit them on the origin server and pull the changes "
                "(backy pull)",
                revisions=",".join(r.uuid for r in remote),
            )
            raise RuntimeError("Remote revs disallowed")

    @locked(target=".backup", mode="exclusive")
    def run_with_backup_lock(self, fun, *args, **kw):
        return fun(*args, **kw)

    #################
    # Making backups

    @locked(target=".backup", mode="exclusive")
    def _clean(self) -> None:
        """Clean-up incomplete revisions."""
        for revision in self.local_history:
            if "duration" not in revision.stats:
                self.log.warning(
                    "clean-incomplete", revision_uuid=revision.uuid
                )
                revision.remove()

    @locked(target=".backup", mode="exclusive")
    def forget(self, revision: str) -> None:
        for r in self.find_revisions(revision):
            r.remove()

    @locked(target=".backup", mode="exclusive")
    def expire(self):
        self.schedule.expire(self)

    @locked(target=".backup", mode="exclusive")
    def tags(
        self,
        action: Literal["set", "add", "remove"],
        revision: str,
        tags: set[str],
        expect: Optional[set[str]] = None,
        autoremove: bool = False,
        force=False,
    ) -> bool:
        self.scan()
        revs = self.find_revisions(revision)
        if not force and action != "remove":
            self.validate_tags(tags)
        for r in revs:
            if expect is not None and expect != r.tags:
                self.log.error("tags-expectation-failed")
                return False
        for r in revs:
            match action:
                case "set":
                    r.tags = tags
                case "add":
                    r.tags |= tags
                case "remove":
                    r.tags -= tags
                case _:
                    raise ValueError(f"invalid action '{action}'")
            if not r.tags and autoremove:
                r.remove()
            else:
                r.write_info()
        return True

    @locked(target=".backup", mode="exclusive")
    def distrust(self, revision: str) -> None:
        revs = self.find_revisions(revision)
        self.prevent_remote_rev(revs)
        for r in revs:
            r.distrust()
            r.write_info()

    ######################
    # Looking up revisions

    def last_by_tag(self) -> dict[str, datetime.datetime]:
        """Return a dictionary showing the last time each tag was
        backed up.

        Tags that have never been backed up won't show up here.

        """
        last_times: dict[str, datetime.datetime] = {}
        for revision in self.clean_history:
            for tag in revision.tags:
                last_times.setdefault(tag, min_date())
                last_times[tag] = max([last_times[tag], revision.timestamp])
        return last_times

    def find_revisions(
        self, spec: str | List[str | Revision | List[Revision]]
    ) -> List[Revision]:
        """Get a sorted list of revisions, oldest first, that match the given
        specification.
        """

        tokens: List[str | Revision | List[Revision]]
        if isinstance(spec, str):
            tokens = [
                t.strip()
                for t in re.split(r"(\(|\)|,|&|\.\.)", spec)
                if t.strip()
            ]
        else:
            tokens = spec
        if "(" in tokens and ")" in tokens:
            i = list_rindex(tokens, "(")
            j = tokens.index(")", i)
            prev, middle, next = tokens[:i], tokens[i + 1 : j], tokens[j + 1 :]

            functions = {
                "first": lambda x: x[0],
                "last": lambda x: x[-1],
                "not": lambda x: [r for r in self.history if r not in x],
                "reverse": lambda x: list(reversed(x)),
            }
            if prev and isinstance(prev[-1], str) and prev[-1] in functions:
                return self.find_revisions(
                    prev[:-1]
                    + [functions[prev[-1]](self.find_revisions(middle))]
                    + next
                )
            return self.find_revisions(
                prev + [self.find_revisions(middle)] + next
            )
        elif "," in tokens:
            i = tokens.index(",")
            return unique(
                self.find_revisions(tokens[:i])
                + self.find_revisions(tokens[i + 1 :])
            )
        elif "&" in tokens:
            i = tokens.index("&")
            return duplicates(
                self.find_revisions(tokens[:i]),
                self.find_revisions(tokens[i + 1 :]),
            )
        elif ".." in tokens:
            _a, _b = list_split(tokens, "..")
            assert len(_a) <= 1 and len(_b) <= 1
            a = self.index_by_token(list_get(_a, 0, "first"))
            b = self.index_by_token(list_get(_b, 0, "last"))
            return self.history[ceil(min(a, b)) : floor(max(a, b)) + 1]
        assert len(tokens) == 1
        token = tokens[0]
        if isinstance(token, Revision):
            return [token]
        elif isinstance(token, list):
            return token
        if token.startswith("server:"):
            server = token.removeprefix("server:")
            return [r for r in self.history if server == r.server]
        elif token.startswith("tag:"):
            tag = token.removeprefix("tag:")
            return [r for r in self.history if tag in r.tags]
        elif token.startswith("trust:"):
            trust = Trust(token.removeprefix("trust:").lower())
            return [r for r in self.history if trust == r.trust]
        elif token == "all":
            return self.history[:]
        elif token == "clean":
            return self.clean_history
        elif token == "local":
            return self.find_revisions("server:")
        elif token == "remote":
            return self.find_revisions("not(server:)")
        else:
            return [self.find(token)]

    def index_by_token(self, spec: str | Revision | List[Revision]) -> float:
        assert not isinstance(
            spec, list
        ), "can only index a single revision specifier"
        if isinstance(spec, str):
            return self.index_by_date(spec) or self.history.index(
                self.find(spec)
            )
        else:
            return self.history.index(spec)

    def index_by_date(self, spec: str) -> Optional[float]:
        """Return index of revision matched by datetime.
        Index may be fractional if there is no exact datetime match.
        Index range: [-0.5, len+0.5]
        """
        try:
            date = datetime.datetime.fromisoformat(spec)
            date = date.replace(tzinfo=date.tzinfo or tzlocal.get_localzone())
            L = list_get(
                [i for i, r in enumerate(self.history) if r.timestamp <= date],
                -1,
                -1,
            )
            r = list_get(
                [i for i, r in enumerate(self.history) if r.timestamp >= date],
                0,
                len(self.history),
            )
            print(spec, L, r)
            assert 0 <= r - L <= 1, (
                "can not index with date if multiple revision have the same "
                "timestamp"
            )
            return (L + r) / 2.0
        except ValueError:
            return None

    def find_by_number(self, _spec: str) -> Revision:
        """Returns revision by relative number.

        0 is the newest,
        1 is the next older,
        2 is the even next older,
        and so on ...

        Raises IndexError or ValueError if no revision is found.
        """
        spec = int(_spec)
        if spec < 0:
            raise KeyError("Integer revisions must be positive")
        return self.history[-spec - 1]

    def find_by_tag(self, spec: str) -> Revision:
        """Returns the latest revision matching a given tag.

        Raises IndexError or ValueError if no revision is found.
        """
        if spec in ["last", "latest"]:
            return self.history[-1]
        if spec == "first":
            return self.history[0]
        raise ValueError()

    def find_by_uuid(self, spec: str) -> Revision:
        """Returns revision matched by UUID.

        Raises IndexError if no revision is found.
        """
        try:
            return self._by_uuid[spec]
        except KeyError:
            raise IndexError()

    def find_by_function(self, spec: str) -> Revision:
        m = re.fullmatch(r"(\w+)\(.+\)", spec)
        if m and m.group(1) in ["first", "last"]:
            return self.find_revisions(m.group(0))[0]
        raise ValueError()

    def find(self, spec: str) -> Revision:
        """Flexible revision search.

        Locates a revision by relative number, by tag, or by uuid.

        """
        spec = spec.strip()
        if spec == "" or not self.history:
            raise KeyError(spec)

        for find in (
            self.find_by_number,
            self.find_by_uuid,
            self.find_by_tag,
            self.find_by_function,
        ):
            try:
                return find(spec)
            except (ValueError, IndexError):
                pass
        self.log.warning("find-rev-not-found", spec=spec)
        raise KeyError(spec)
