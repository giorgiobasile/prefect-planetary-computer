import json

import pytest
import requests_mock
from dask.distributed import LocalCluster
from importlib_resources import files
from prefect.testing.utilities import prefect_test_harness

from prefect_planetary_computer.credentials import (
    CATALOG_URL,
    PlanetaryComputerCredentials,
)
from prefect_planetary_computer.gateway import PlanetaryComputerCluster


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
def mock_pc_stac_responses():
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
def mock_gateway_cluster():
    class MyLocalCluster(LocalCluster):
        def __init__(self, *args, **kwargs):
            super().__init__(name="test-cluster")

    PlanetaryComputerCluster.__bases__ = (MyLocalCluster,)


@pytest.fixture
def mock_pc_credentials_block(mock_pc_stac_responses, mock_gateway_cluster):  # noqa
    return PlanetaryComputerCredentials(
        hub_api_token="fake-token", subscription_key="fake-key"
    )
