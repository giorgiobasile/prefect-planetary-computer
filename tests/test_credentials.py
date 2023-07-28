from dask_gateway import Gateway
from pystac_client import Client


def test_get_dask_gateway(pc_credentials_block):
    gateway_client = pc_credentials_block.get_dask_gateway()
    assert isinstance(gateway_client, Gateway)
    assert gateway_client.address == "127.0.0.1"


def test_get_stac_catalog(pc_credentials_block):
    stac_catalog = pc_credentials_block.get_stac_catalog()
    assert isinstance(stac_catalog, Client)
    assert stac_catalog.id == "microsoft-pc"
