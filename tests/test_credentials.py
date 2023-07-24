from dask_gateway import Gateway


def test_get_gateway_client(pc_credentials_block):
    gateway_client = pc_credentials_block.get_gateway_client()
    assert isinstance(gateway_client, Gateway)
    assert gateway_client.address == "127.0.0.1"
