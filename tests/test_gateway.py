import pytest

from prefect_planetary_computer.constants import GATEWAY_ADDRESS, GATEWAY_PROXY_ADDRESS
from prefect_planetary_computer.gateway import (
    PlanetaryComputerCluster,
    PlanetaryComputerGateway,
)


def test_pc_gateway():
    with pytest.raises(ValueError):
        gw = PlanetaryComputerGateway(hub_api_token="test-token", address="test")
        gw = PlanetaryComputerGateway(hub_api_token="test-token", proxy_address="test")

    gw = PlanetaryComputerGateway(hub_api_token="test-token")
    assert gw.address == GATEWAY_ADDRESS
    assert gw.proxy_address == GATEWAY_PROXY_ADDRESS


def test_pc_gateway_cluster(mock_gateway_cluster):
    gateway_cluster = PlanetaryComputerCluster(
        hub_api_token="test-token",
        worker_cores=1.0,
        worker_memory=8.0,
        image="pangeo/pangeo-notebook:latest",
        gpu=False,
        environment={"GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR"},
    )
    assert gateway_cluster.name == "test-cluster"
