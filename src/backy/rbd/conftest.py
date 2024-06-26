import json
import os

import pytest

from backy.rbd import RbdRepository
from backy.revision import Revision

fixtures = os.path.dirname(__file__) + "/tests/samples"


@pytest.fixture
def rbdrepository(schedule, tmp_path, log):
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
    return RbdRepository(tmp_path, log)


def create_rev(rbdrepository, tags):
    r = Revision.create(rbdrepository, tags, rbdrepository.log)
    r.materialize()
    rbdrepository.scan()
    return r
