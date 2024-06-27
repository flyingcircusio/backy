from typing import cast

from backy.file import FileSource
from backy.repository import Repository
from backy.revision import Revision


def test_simple_cycle(tmp_path, log):
    original = tmp_path / "original.txt"
    with open(original, "w") as f:
        f.write("This is the original file.")

    repo_path = tmp_path / "repository"

    repository = Repository.init(
        repo_path, FileSource, FileSource.to_config(repo_path), log
    )
    source = cast(FileSource, repository.get_source())

    revision = Revision.create(repository, {"test"}, log)
    source.backup(revision)

    with open(original, "w") as f:
        f.write("This is the wrong file.")

    assert original.read_text() == "This is the wrong file."

    source.restore(revision, original)

    assert original.read_text() == "This is the original file."
