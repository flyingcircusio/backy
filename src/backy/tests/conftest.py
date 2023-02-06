from zoneinfo import ZoneInfo

import pytest
import tzlocal


@pytest.fixture
def tz_berlin(monkeypatch):
    """Fix time zone to gain independece from runtime environment."""
    monkeypatch.setattr(
        tzlocal, "get_localzone", lambda: ZoneInfo("Europe/Berlin")
    )
