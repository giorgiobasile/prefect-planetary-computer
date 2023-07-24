import pytest
from prefect.testing.utilities import prefect_test_harness

from prefect_planetary_computer.credentials import PlanetaryComputerCredentials


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    """
    Sets up test harness for temporary DB during test runs.
    """
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def reset_object_registry():
    """
    Ensures each test has a clean object registry.
    """
    from prefect.context import PrefectObjectRegistry

    with PrefectObjectRegistry():
        yield


@pytest.fixture
def pc_credentials_block(monkeypatch):
    monkeypatch.setattr(
        "prefect_planetary_computer.credentials.GATEWAY_ADDRESS", "127.0.0.1"
    )
    return PlanetaryComputerCredentials(hub_api_token="fake-token")
