import json
import os

import pytest

from backy.rbd import RbdBackup

fixtures = os.path.dirname(__file__) + "/tests/samples"


@pytest.fixture
def rbdbackup(schedule, tmp_path, log):
    with open(str(tmp_path / "config"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "source": {
                    "type": "file",
                    "filename": "test",
                },
                "schedule": schedule.to_dict(),
            },
            f,
        )
    return RbdBackup(tmp_path, log)
