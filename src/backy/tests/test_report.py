from backy.report import ChunkMismatchReport
from backy.tests import Ellipsis


def test_report(tmp_path, repository, log, clock):
    repository.add_report(ChunkMismatchReport(b"source", b"target", 3))
    with open(
        (tmp_path / "quarantine" / repository.report_ids[0]).with_suffix(
            ".report"
        )
    ) as report:
        assert (
            Ellipsis(
                f"""\
uuid: {repository.report_ids[0]}
timestamp: 2015-09-01 07:06:47+00:00
source_hash: 36cd38f49b9afa08222c0dc9ebfe35eb
target_hash: 42aefbae01d2dfd981f7da7d823d689e
offset: 3
traceback: |-
...
    File ".../src/backy/tests/test_report.py", line ..., in test_report
      repository.add_report(ChunkMismatchReport(b"source", b"target", 3))
    File ".../src/backy/report.py", line ..., in __init__
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
