import yaml

from backy.file import FileSource
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule


def test_bootstrap_from_api(tmp_path, log):
    original = tmp_path / "original.txt"

    schedule = Schedule()
    repository = Repository(tmp_path / "repository", FileSource, schedule, log)
    repository.connect()
    source = FileSource(repository, original)

    exercise_fresh_repo(source)


def test_bootstrap_from_config(tmp_path, log):
    original = tmp_path / "original.txt"

    repo_path = tmp_path / "repository"

    repo_conf = {
        "path": repo_path,
        "schedule": {},
        "type": "file",
    }
    source_conf = {"type": "file", "path": str(original)}

    repository = Repository.from_config(repo_conf, log)
    repository.connect()
    source = FileSource.from_config(repository, source_conf, log)

    exercise_fresh_repo(source)


def exercise_fresh_repo(source: FileSource):
    original = source.path

    with open(original, "w") as f:
        f.write("This is the original file.")

    revision = Revision.create(
        source.repository, {"test"}, source.repository.log
    )
    source.backup(revision)

    with open(original, "w") as f:
        f.write("This is the wrong file.")

    assert original.read_text() == "This is the wrong file."

    source.restore(revision, original)

    assert original.read_text() == "This is the original file."
