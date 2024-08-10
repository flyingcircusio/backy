# Placeholder for future S3 implementation
from argparse import _ActionsContainer
from dataclasses import dataclass
from typing import Any, Iterable

from structlog.stdlib import BoundLogger

from backy.repository import Repository
from backy.revision import Revision
from backy.source import RestoreArgs, RestoreArgsType, Source, SourceType


@dataclass(frozen=True)
class S3RestoreArgs(RestoreArgs):
    def to_cmdargs(self) -> Iterable[str]:
        return []

    @classmethod
    def setup_argparse(cls, restore_parser: _ActionsContainer) -> None:
        pass

    @classmethod
    def from_args(cls: type[RestoreArgsType], **kw: Any) -> RestoreArgsType:
        return cls()


class S3Source(Source):
    type_ = "s3"
    restore_type = S3RestoreArgs

    @classmethod
    def from_config(
        cls: type[SourceType],
        repository: "Repository",
        config: dict[str, Any],
        log: BoundLogger,
    ) -> SourceType:
        raise NotImplementedError()

    def backup(self, revision: "Revision") -> bool:
        raise NotImplementedError()

    def restore(self, revision: "Revision", args: RestoreArgsType):
        raise NotImplementedError()

    def verify(self, revision: "Revision"):
        raise NotImplementedError()

    def gc(self) -> None:
        raise NotImplementedError()


def main():
    raise NotImplementedError()
