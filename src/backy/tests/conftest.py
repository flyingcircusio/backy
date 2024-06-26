import json

import pytest

from backy.repository import Repository


@pytest.fixture
def repository(schedule, tmp_path, log):
    with open(str(tmp_path / "config"), "w", encoding="utf-8") as f:
        json.dump({"schedule": schedule.to_dict()}, f)
    return Repository(tmp_path, log)
