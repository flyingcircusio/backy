from backy.rbd.quarantine import QuarantineReport, QuarantineStore
from backy.tests import Ellipsis


def test_quarantine(tmp_path, log, clock):
    store = QuarantineStore(tmp_path, log)
    store.add_report(QuarantineReport(b"source", b"target", 3))
    with open(
        (tmp_path / "quarantine" / store.report_ids[0]).with_suffix(".report")
    ) as report:
        assert (
            Ellipsis(
                f"""\
uuid: {store.report_ids[0]}
source_hash: 36cd38f49b9afa08222c0dc9ebfe35eb
target_hash: 42aefbae01d2dfd981f7da7d823d689e
offset: 3
timestamp: 2015-09-01 07:06:47+00:00
traceback: |-
...
    File ".../src/backy/tests/test_quarantine.py", line ..., in test_quarantine
      store.add_report(QuarantineReport(b"source", b"target", 3))
    File ".../src/backy/quarantine.py", line ..., in __init__
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
