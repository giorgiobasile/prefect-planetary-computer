"""Module handling Microsoft Planetary Computer credentials"""

from typing import Optional

import dask_gateway
from dask_gateway.auth import JupyterHubAuth
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

GATEWAY_ADDRESS = (
    "https://pccompute.westeurope.cloudapp.azure.com/compute/services/dask-gateway"
)
GATEWAY_PROXY_ADDRESS = "gateway://pccompute-dask.westeurope.cloudapp.azure.com:80"
CLUSTER_IMAGES_BASE = "mcr.microsoft.com/planetary-computer/"
CLUSTER_DEFAULT_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MAX_RETRY": "5",
    "GDAL_HTTP_RETRY_DELAY": "3",
    "USE_PYGEOS": "0",
}


class PlanetaryComputerCredentials(Block):
    """
    Block used to manage Microsoft Planetary Computer credentials.

    The block stores a [subscription key](https://planetarycomputer.microsoft.com/docs/concepts/sas#when-an-account-is-needed)
    to access the full PC data catalog and a
    [JupyterHub API token](https://planetarycomputer.microsoft.com/docs/concepts/computing/#request-a-token-from-jupyterhub)
    to instantiate clusters through Dask Gateway.

    Example:
        Load stored Planetary Computer credentials:
        ```python
        from prefect_planetary_computer import PlanetaryComputerCredentials
        pc_credentials_block = PlanetaryComputerCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    _block_type_name = "Planetary Computer Credentials"
    _logo_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Microsoft_logo.svg/512px-Microsoft_logo.svg.png"  # noqa
    _documentation_url = "https://giorgiobasile.github.io/prefect-planetary-computer/blocks/#prefect_planetary_computer.blocks.PlanetarycomputerBlock"  # noqa

    subscription_key: Optional[SecretStr] = Field(
        default=None,
        description="A subscription key to access the full PC data catalog.",
        title="PC Subscription Key",
    )
    hub_api_token: Optional[SecretStr] = Field(
        default=None,
        description="The JupyterHub API token to instantiate clusters through Dask Gateway.",  # noqa E501
        title="JupyterHub API Token",
    )

    def get_gateway_client(self, **gateway_kwargs) -> dask_gateway.Gateway:
        """
        Provides a client for the PC Dask Gateway Server,
        setting the proper addresses and Jupyter authentication.

        Args:
            gateway_kwargs: Additional keyword arguments to pass
                to the Dask Gateway client.

        Returns:
            A Dask Gateway client to instantiate clusters.

        Example:
            Get a configured Dask Gateway client:
            ```python
            from prefect_planetary_computer.earthdata import EarthdataCredentials

            pc_credentials_block = PlanetaryComputerCredentials(
                subscription_key = "sub-key",
                hub_api_token = "hub-token"
            )
            gateway_client = pc_credentials_block.get_gateway_client()

            # List available clusters
            gateway_client.list_clusters()
            ```
        """  # noqa E501
        if self.hub_api_token is None:
            raise ValueError("JupyterHub API Token hasn't been provided.")

        return dask_gateway.Gateway(
            address=GATEWAY_ADDRESS,
            proxy_address=GATEWAY_PROXY_ADDRESS,
            auth=JupyterHubAuth(api_token=self.hub_api_token.get_secret_value()),
            **gateway_kwargs
        )
