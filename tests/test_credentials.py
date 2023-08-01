from dask_gateway import Gateway, GatewayCluster
from pystac_client import Client

from prefect_planetary_computer.credentials import GATEWAY_ADDRESS


def test_get_dask_gateway(pc_credentials_block):
    gateway_client = pc_credentials_block.get_dask_gateway()
    assert isinstance(gateway_client, Gateway)
    assert gateway_client.address == GATEWAY_ADDRESS


def test_new_dask_gateway_cluster(pc_credentials_block):
    gateway_cluster = pc_credentials_block.new_dask_gateway_cluster(
        worker_cores=1.0,
        worker_memory=8.0,
        image="pangeo/pangeo-notebook:latest",
        gpu=False,
        environment={"GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR"},
    )
    assert isinstance(gateway_cluster, GatewayCluster)
    assert gateway_cluster.name == "test-cluster"


def test_get_stac_catalog(pc_credentials_block):
    stac_catalog = pc_credentials_block.get_stac_catalog()
    assert isinstance(stac_catalog, Client)
    assert stac_catalog.id == "microsoft-pc"