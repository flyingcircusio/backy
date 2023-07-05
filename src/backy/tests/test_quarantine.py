from backy.quarantine import QuarantineReport, QuarantineStore
from backy.tests import Ellipsis


def test_quarantine(tmpdir, log, clock):
    store = QuarantineStore(tmpdir, log)
    store.add_report(QuarantineReport(b"source", b"target", 3))
    with open(
        tmpdir / "quarantine" / store.report_ids[0] + ".report"
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
        tmpdir / "quarantine" / "chunks" / "36cd38f49b9afa08222c0dc9ebfe35eb"
    ) as source:
        assert "source" == source.read()

    with open(
        tmpdir / "quarantine" / "chunks" / "42aefbae01d2dfd981f7da7d823d689e"
    ) as target:
        assert "target" == target.read()
