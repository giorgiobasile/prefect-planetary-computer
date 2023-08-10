import pytest
from dask_gateway import Gateway
from prefect_dask import DaskTaskRunner
from pystac_client import Client

from prefect_planetary_computer.credentials import (
    GATEWAY_ADDRESS,
    PlanetaryComputerCredentials,
)


def test_eq(mock_pc_credentials_block):
    other_str = "test-str"
    other_cr = PlanetaryComputerCredentials()
    assert mock_pc_credentials_block != other_str
    assert mock_pc_credentials_block != other_cr


def test_get_stac_catalog(mock_pc_credentials_block):
    stac_catalog = mock_pc_credentials_block.get_stac_catalog(sign_inplace=True)
    assert isinstance(stac_catalog, Client)
    assert stac_catalog.id == "microsoft-pc"


def test_get_gateway_fail():
    with pytest.raises(ValueError):
        PlanetaryComputerCredentials().get_gateway()


def test_get_gateway(mock_pc_credentials_block):
    gateway_client = mock_pc_credentials_block.get_gateway()
    assert isinstance(gateway_client, Gateway)
    assert gateway_client.address == GATEWAY_ADDRESS


def test_new_gateway_cluster(mock_pc_credentials_block):
    gateway_cluster = mock_pc_credentials_block.new_gateway_cluster(
        worker_cores=1.0,
        worker_memory=8.0,
        image="pangeo/pangeo-notebook:latest",
        gpu=False,
        environment={"GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR"},
    )
    assert gateway_cluster.name == "test-cluster"


def test_get_dask_task_runner(mock_pc_credentials_block):
    task_runner = mock_pc_credentials_block.get_dask_task_runner()
    assert isinstance(task_runner, DaskTaskRunner)
    assert task_runner.cluster_kwargs["address"] == GATEWAY_ADDRESS
