"""Module handling Microsoft Planetary Computer credentials"""

from typing import Optional

import dask_gateway
import planetary_computer
import pystac_client
from dask_gateway.auth import JupyterHubAuth
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

CATALOG_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/"
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

    def get_dask_gateway(self, **gateway_kwargs) -> dask_gateway.Gateway:
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
            from prefect import flow
            from prefect_planetary_computer import PlanetaryComputerCredentials

            @flow
            def example_get_dask_gateway_flow():
                pc_credentials_block = PlanetaryComputerCredentials(
                    subscription_key = "sub-key",
                    hub_api_token = "hub-token"
                )
                gateway = pc_credentials_block.get_dask_gateway()

                # List available clusters
                clusters = gateway.list_clusters()
                return len(clusters)

            example_get_dask_gateway_flow()
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

    def get_stac_catalog(self, **pystac_kwargs) -> pystac_client.Client:
        """
        Provides a PySTAC client for the PC data catalog, automatically signing items as they are retrieved.
        For more information about PC signing, refer to the [docs](https://planetarycomputer.microsoft.com/docs/concepts/sas).

        Args:
            pystac_kwargs: Additional keyword arguments to pass to the [pystac_client.Client.open](https://pystac-client.readthedocs.io/en/stable/api.html#pystac_client.Client.open) method.

        Returns:
            A PySTAC client for the PC Catalog.

        Example:
            Get a configured PySTAC client with automatic asset signing:
            ```python
            from prefect import flow
            from prefect_planetary_computer import PlanetaryComputerCredentials

            @flow
            def example_get_stac_catalog_flow():
                pc_credentials_block = PlanetaryComputerCredentials(
                    subscription_key = "sub-key",
                    hub_api_token = "hub-token"
                )
                catalog = pc_credentials_block.get_stac_catalog()

                # Search STAC catalog for Landsat Collection 2 Level 2 items
                time_range = "2020-12-01/2020-12-31"
                bbox = [-122.2751, 47.5469, -121.9613, 47.7458]

                search = catalog.search(collections=["landsat-c2-l2"], bbox=bbox, datetime=time_range)
                items = search.get_all_items()
                return len(items)

            example_get_stac_catalog_flow()
            ```
        """  # noqa E501

        if self.subscription_key:
            planetary_computer.set_subscription_key(
                self.subscription_key.get_secret_value()
            )

        return pystac_client.Client.open(
            CATALOG_URL, modifier=planetary_computer.sign_inplace, **pystac_kwargs
        )
