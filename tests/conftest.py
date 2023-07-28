import json

import pytest
import requests_mock
from importlib_resources import files
from prefect.testing.utilities import prefect_test_harness

from prefect_planetary_computer.credentials import (
    CATALOG_URL,
    PlanetaryComputerCredentials,
)


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
def pc_stac_mock_responses():
    with requests_mock.Mocker() as m:
        with files("data").joinpath("stac-catalog-response.json").open(
            "r"
        ) as stac_catalog_response_file:
            stac_catalog_response = json.load(stac_catalog_response_file)

            m.get(
                CATALOG_URL,
                json=stac_catalog_response,
                status_code=200,
            )
            yield m


@pytest.fixture
def pc_credentials_block(monkeypatch, pc_stac_mock_responses):  # noqa
    monkeypatch.setattr(
        "prefect_planetary_computer.credentials.GATEWAY_ADDRESS", "127.0.0.1"
    )
    return PlanetaryComputerCredentials(hub_api_token="fake-token")
