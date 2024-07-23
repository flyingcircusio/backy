from backy.report import ChunkMismatchReport
from backy.repository import Repository
from backy.tests import Ellipsis


def test_quarantine(tmp_path, log, clock):
    repo = Repository()
    repo.add_report(ChunkMismatchReport(b"source", b"target", 3))
    with open(
        (tmp_path / "quarantine" / repo.report_ids[0]).with_suffix(".report")
    ) as report:
        assert (
            Ellipsis(
                f"""\
uuid: {repo.report_ids[0]}
source_hash: 36cd38f49b9afa08222c0dc9ebfe35eb
target_hash: 42aefbae01d2dfd981f7da7d823d689e
offset: 3
timestamp: 2015-09-01 07:06:47+00:00
traceback: |-
...
    File ".../src/backy/rbd/tests/test_quarantine.py", line ..., in test_quarantine
      store.add_report(QuarantineReport(b"source", b"target", 3))
    File ".../src/backy/rbd/quarantine.py", line ..., in __init__
      self.traceback = "".join(traceback.format_stack()).strip()
"""
            )
            == report.read()
        )

    with open(
        tmp_path / "quarantine" / "chunks" / "36cd38f49b9afa08222c0dc9ebfe35eb"
    ) as source:
        assert "source" == source.read()

    with open(
        tmp_path / "quarantine" / "chunks" / "42aefbae01d2dfd981f7da7d823d689e"
    ) as target:
        assert "target" == target.read()
