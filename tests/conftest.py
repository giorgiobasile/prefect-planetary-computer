import json

import pytest
import requests_mock
from dask.distributed import LocalCluster
from dask_gateway import Gateway
from importlib_resources import files
from prefect.testing.utilities import prefect_test_harness
from prefect.utilities.importtools import from_qualified_name

from prefect_planetary_computer.credentials import (
    CATALOG_URL,
    PlanetaryComputerCredentials,
)
from prefect_planetary_computer.task_runners import PlanetaryComputerTaskRunner


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
def mock_gateway_new_cluster(monkeypatch):
    def mock_new_cluster(*args, **kwargs):  # noqa
        cluster = LocalCluster(name="test-cluster")
        return cluster

    monkeypatch.setattr(Gateway, "new_cluster", mock_new_cluster)


@pytest.fixture
def mock_pc_credentials_block(mock_pc_stac_responses, mock_gateway_new_cluster):  # noqa
    return PlanetaryComputerCredentials(
        hub_api_token="fake-token", subscription_key="fake-key"
    )


@pytest.fixture
def mock_pc_task_runner(mock_pc_credentials_block):
    def new_basic_mock_pc_runner():
        basic_pc_runner = PlanetaryComputerTaskRunner(mock_pc_credentials_block)
        basic_pc_runner.cluster_class = from_qualified_name(
            "dask.distributed.LocalCluster"
        )
        basic_pc_runner.cluster_kwargs = {"name": "test-cluster"}
        return basic_pc_runner

    pc_runner = new_basic_mock_pc_runner()
    pc_runner.duplicate = new_basic_mock_pc_runner

    return pc_runner
