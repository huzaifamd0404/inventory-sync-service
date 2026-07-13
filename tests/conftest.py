import os

import pytest


@pytest.fixture(autouse=True)
def isolate_integration_env() -> None:
    os.environ.setdefault("RUN_E2E_INTEGRATION_TESTS", "false")
