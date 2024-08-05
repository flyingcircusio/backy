from backy.file import FileRestoreArgs, FileSource
from backy.repository import Repository
from backy.revision import Revision
from backy.schedule import Schedule
from backy.source import CmdLineSource


def test_bootstrap_from_api(tmp_path, log):
    original = tmp_path / "original.txt"

    schedule = Schedule()
    repository = Repository(tmp_path / "repository", schedule, log)
    repository.connect()
    source = FileSource(repository, original)

    exercise_fresh_repo(source)


def test_bootstrap_from_config(tmp_path, log):
    original = tmp_path / "original.txt"

    repo_path = tmp_path / "repository"

    conf = {
        "path": repo_path,
        "schedule": {},
        "source": {"type": "file", "filename": str(original)},
    }

    source = CmdLineSource.from_config(conf, log).create_source(FileSource)

    exercise_fresh_repo(source)


def exercise_fresh_repo(source: FileSource):
    original = source.filename

    with open(original, "w") as f:
        f.write("This is the original file.")

    revision = Revision.create(
        source.repository, {"test"}, source.repository.log
    )
    source.backup(revision)

    with open(original, "w") as f:
        f.write("This is the wrong file.")

    assert original.read_text() == "This is the wrong file."

    source.restore(revision, FileRestoreArgs(original))

    assert original.read_text() == "This is the original file."
