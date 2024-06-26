import json
import os

import pytest

from backy.rbd import RbdSource
from backy.revision import Revision

fixtures = os.path.dirname(__file__) + "/tests/samples"


@pytest.fixture
def rbdbackup(schedule, tmp_path, log):
    with open(str(tmp_path / "config"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "source": {
                    "type": "file",
                    "filename": "input-file",
                },
                "schedule": schedule.to_dict(),
            },
            f,
        )
    return RbdSource(tmp_path, log)


def create_rev(rbdbackup, tags):
    r = Revision.create(rbdbackup, tags, rbdbackup.log)
    r.materialize()
    rbdbackup.scan()
    return r
