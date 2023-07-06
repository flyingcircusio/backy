# This file is dual licensed under the terms of the Apache License, Version
# 2.0, and the MIT License.  See the LICENSE file in the root of this
# repository for complete details.

import io
import os
import string
import sys
from pathlib import Path
from typing import Optional

import structlog

try:
    import colorama
except ImportError:
    colorama = None

_MISSING = "{who} requires the {package} package installed."
_EVENT_WIDTH = 30  # pad the event name to so many characters

if sys.stderr.isatty() and colorama:
    COLORIZED_TTY_OUTPUT = True

    RESET_ALL = colorama.Style.RESET_ALL
    BRIGHT = colorama.Style.BRIGHT
    DIM = colorama.Style.DIM
    RED = colorama.Fore.RED
    BACKRED = colorama.Back.RED
    BLUE = colorama.Fore.BLUE
    CYAN = colorama.Fore.CYAN
    MAGENTA = colorama.Fore.MAGENTA
    YELLOW = colorama.Fore.YELLOW
    GREEN = colorama.Fore.GREEN
else:
    COLORIZED_TTY_OUTPUT = False

    RESET_ALL = ""
    BRIGHT = ""
    DIM = ""
    RED = ""
    BACKRED = ""
    BLUE = ""
    CYAN = ""
    MAGENTA = ""
    YELLOW = ""
    GREEN = ""


class PartialFormatter(string.Formatter):
    """
    A string formatter that doesn't break if values are missing or formats are wrong.
    Missing values and bad formats are replaced by a fixed string that can be set
    when constructing an formatter object.

    formatter = PartialFormatter(missing='<missing>', bad_format='<bad format'>)
    formatted_str = formatter.format("{exists} {missing}", exists=1)
    formatted_str == "1 <missing>"
    """

    def __init__(self, missing="<missing>", bad_format="<bad format>"):
        self.missing = missing
        self.bad_format = bad_format

    def get_field(self, field_name, args, kwargs):
        try:
            val = super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            val = (None, field_name)
        return val

    def format_field(self, value, format_spec):
        if value is None:
            return self.missing
        try:
            return super().format_field(value, format_spec)
        except ValueError:
            return self.bad_format


class MultiOptimisticLoggerFactory:
    def __init__(self, **factories):
        self.factories = factories

    def __call__(self, *args):
        loggers = {k: f() for k, f in self.factories.items()}
        return MultiOptimisticLogger(loggers)


class MultiOptimisticLogger:
    """
    A logger which distributes messages to multiple loggers.
    It's initialized with a logger dict where the keys are the logger names
    which correspond to the keyword arguments given to the msg method.
    If the logger's name is not present in the arguments, the logger is skipped.
    Errors in sub loggers are ignored silently.
    """

    def __init__(self, loggers):
        self.loggers = loggers

    def __repr__(self):
        return "<MultiOptimisticLogger {}>".format(
            [repr(l) for l in self.loggers]
        )

    def msg(self, **messages):
        for name, logger in self.loggers.items():
            line = messages.get(name)
            if line:
                logger.msg(line)

    def __getattr__(self, name):
        return self.msg


def prefix(prefix, line):
    return "{}>\t".format(prefix) + line.replace("\n", "\n{}>\t".format(prefix))


class ConsoleFileRenderer:
    """
    Render `event_dict` nicely aligned, in colors, and ordered with
    specific knowledge about backy structures.
    """

    LEVELS = [
        "alert",
        "critical",
        "error",
        "warn",
        "warning",
        "info",
        "debug",
        "trace",
    ]

    def __init__(
        self, min_level, default_job_name: str = "", pad_event=_EVENT_WIDTH
    ):
        self.min_level = self.LEVELS.index(min_level.lower())
        self.default_job_name = default_job_name
        if colorama is None:
            print(
                _MISSING.format(who=self.__class__.__name__, package="colorama")
            )
        if COLORIZED_TTY_OUTPUT:
            colorama.init()

        self._pad_event = pad_event
        self._level_to_color = {
            "alert": RED,
            "critical": RED,
            "error": RED,
            "warn": YELLOW,
            "warning": YELLOW,
            "info": GREEN,
            "debug": GREEN,
            "trace": GREEN,
            "notset": BACKRED,
        }
        for key in self._level_to_color.keys():
            self._level_to_color[key] += BRIGHT
        self._longest_level = len(
            max(self._level_to_color.keys(), key=lambda e: len(e))
        )

    def __call__(self, logger, method_name, event_dict):
        console_io = io.StringIO()
        log_io = io.StringIO()

        def write(line):
            console_io.write(line)
            if RESET_ALL:
                for SYMB in [
                    RESET_ALL,
                    BRIGHT,
                    DIM,
                    RED,
                    BACKRED,
                    BLUE,
                    CYAN,
                    MAGENTA,
                    YELLOW,
                    GREEN,
                ]:
                    line = line.replace(SYMB, "")
            log_io.write(line)

        fmt_msg = event_dict.pop("_fmt_msg", None)
        if fmt_msg:
            formatter = PartialFormatter()
            fmt_msg = formatter.format(fmt_msg, **event_dict)

        ts = event_dict.pop("timestamp", None)
        if ts is not None:
            write(
                DIM
                + str(ts)  # can be a number if timestamp is UNIXy
                + RESET_ALL
                + " "
            )

        pid = event_dict.pop("pid", None)
        if pid is not None:
            write(DIM + str(pid) + RESET_ALL + " ")

        level = event_dict.pop("level", None)
        if level is not None:
            write(
                self._level_to_color[level] + level[0].upper() + RESET_ALL + " "
            )

        job_name = event_dict.pop("job_name", self.default_job_name)
        if job_name:
            write(job_name.ljust(20) + " ")

        subsystem = event_dict.pop("subsystem", "")
        if subsystem:
            subsystem += "/"
        event = event_dict.pop("event")
        write(
            BRIGHT
            + (subsystem + event).ljust(self._pad_event)
            + RESET_ALL
            + " "
        )

        logger_name = event_dict.pop("logger", None)
        if logger_name is not None:
            write("[" + BLUE + BRIGHT + logger_name + RESET_ALL + "] ")

        output = event_dict.pop("_output", None)
        stdout = event_dict.pop("stdout", None)
        stderr = event_dict.pop("stderr", None)
        stack = event_dict.pop("stack", None)
        exception_traceback = event_dict.pop("exception_traceback", None)

        write(
            " ".join(
                CYAN
                + key
                + RESET_ALL
                + "="
                + MAGENTA
                + repr(event_dict[key])
                + RESET_ALL
                for key in sorted(event_dict.keys())
            )
        )

        if fmt_msg is not None:
            write("\n" + prefix("", "\n" + fmt_msg + "\n") + RESET_ALL)

        if output is not None:
            write("\n" + prefix("", "\n" + output + "\n") + RESET_ALL)

        if stdout is not None:
            write("\n" + DIM + prefix("out", "\n" + stdout + "\n") + RESET_ALL)

        if stderr is not None:
            write("\n" + prefix("err", "\n" + stderr + "\n") + RESET_ALL)

        if stack is not None:
            write("\n" + prefix("stack", stack))
            if exception_traceback is not None:
                write("\n" + "=" * 79 + "\n")

        if exception_traceback is not None:
            write("\n" + prefix("exception", exception_traceback))

        # Filter according to the -v switch when outputting to the
        # console.
        if self.LEVELS.index(method_name.lower()) > self.min_level:
            console_io.seek(0)
            console_io.truncate()

        return {"console": console_io.getvalue(), "file": log_io.getvalue()}


def add_pid(logger, method_name, event_dict):
    event_dict["pid"] = os.getpid()
    return event_dict


def process_exc_info(logger, name, event_dict):
    """Transforms exc_info to the exception tuple format returned by
    sys.exc_info(). Uses the the same logic as as structlog's format_exc_info()
    to unify the different types exc_info could contain but doesn't render
    the exception yet.
    """
    exc_info = event_dict.get("exc_info", None)

    if isinstance(exc_info, BaseException):
        event_dict["exc_info"] = (
            exc_info.__class__,
            exc_info,
            exc_info.__traceback__,
        )
    elif isinstance(exc_info, tuple):
        pass
    elif exc_info:
        event_dict["exc_info"] = sys.exc_info()

    return event_dict


def format_exc_info(logger, name, event_dict):
    """Renders exc_info if it's present.
    Expects the tuple format returned by sys.exc_info().
    Compared to structlog's format_exc_info(), this renders the exception
    information separately which is better for structured logging targets.
    """
    exc_info = event_dict.pop("exc_info", None)
    if exc_info is not None:
        exception_class = exc_info[0]
        formatted_traceback = structlog.processors._format_exception(exc_info)
        event_dict["exception_traceback"] = formatted_traceback
        event_dict["exception_msg"] = str(exc_info[1])
        event_dict["exception_class"] = (
            exception_class.__module__ + "." + exception_class.__name__
        )

    return event_dict


def init_logging(
    verbose: bool,
    logfile: Optional[Path] = None,
    default_job_name: str = "",
):

    console_file_renderer = ConsoleFileRenderer(
        min_level="trace" if verbose else "info",
        default_job_name=default_job_name,
    )

    processors = [
        add_pid,
        structlog.processors.add_log_level,
        process_exc_info,
        format_exc_info,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso", utc=False),
        console_file_renderer,
    ]

    loggers = {}

    if logfile is not None:
        loggers["file"] = structlog.PrintLoggerFactory(open(logfile, "a"))

    loggers["console"] = structlog.PrintLoggerFactory(sys.stderr)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=MultiOptimisticLoggerFactory(**loggers),
        cache_logger_on_first_use=False,
    )
