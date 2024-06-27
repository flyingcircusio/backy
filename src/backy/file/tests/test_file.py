from typing import cast

import yaml

from backy.file import FileSource
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule


def test_bootstrap_from_api(tmp_path, log):
    original = tmp_path / "original.txt"

    source = FileSource(original)

    schedule = Schedule()

    repo_path = tmp_path / "repository"
    repository = Repository(repo_path, source, schedule, log)

    exercise_fresh_repo(repository)


def test_bootstrap_from_config(tmp_path, log):
    original = tmp_path / "original.txt"

    repo_path = tmp_path / "repository"

    config = {
        "path": repo_path,
        "schedule": {},
        "source": {"type": "file", "path": str(original)},
    }

    repository = Repository.from_config(config, log)

    exercise_fresh_repo(repository)


def exercise_fresh_repo(repository: Repository):
    source = cast(FileSource, repository.source)

    original = source.path

    repository.connect()

    with open(original, "w") as f:
        f.write("This is the original file.")

    revision = Revision.create(repository, {"test"}, repository.log)
    source.backup(revision)

    with open(original, "w") as f:
        f.write("This is the wrong file.")

    assert original.read_text() == "This is the wrong file."

    source.restore(revision, original)

    assert original.read_text() == "This is the original file."
