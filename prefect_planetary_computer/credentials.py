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
                pc_credentials_block = PlanetaryComputerCredentials.load("BLOCK_NAME")

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

    def get_dask_gateway(self, **gateway_kwargs) -> dask_gateway.Gateway:
        """
        Provides a client for the PC Dask Gateway Server,
        setting the proper addresses and Jupyter authentication.

        For examples on how to use the Dask Gateway client, refer to the [PC - Scale with Dask](https://planetarycomputer.microsoft.com/docs/quickstarts/scale-with-dask/) tutorial.

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
            **gateway_kwargs,
        )

    def new_dask_gateway_cluster(
        self,
        worker_cores: Optional[float] = None,
        worker_memory: Optional[float] = None,
        image: Optional[str] = None,
        gpu: Optional[bool] = False,
        environment: Optional[dict] = None,
        **gateway_cluster_kwargs,
    ) -> dask_gateway.GatewayCluster:
        """
        Instantiate a new cluster from the PC Dask Gateway Server.

        Please make sure to **share the same python dependencies** as the Docker image used for the Dask workers,
        [as explained in the Dask docs](https://docs.dask.org/en/stable/how-to/manage-environments.html#manage-environments).

        Each argument corresponds to one of the available PC Dask Gateway cluster option.
        PC sets some defaults, but they can be overridden by passing the corresponding argument to this function -
        [see Helm chart](https://github.com/microsoft/planetary-computer-hub/blob/main/helm/chart/config.yaml).

        Args:
            worker_cores: Number of cores per worker, in the 0.1-8 range. Defaults to 1.
            worker_memory: Amount of memory per worker (in GiB) in the 1-64 range. Defaults to 8.
            image: The Docker image to be used for the workers. Defaults to [pangeo/pangeo-notebook:latest](https://hub.docker.com/layers/pangeo/pangeo-notebook/latest/images/sha256-94e97e24adf14e72c01f18c782b8c4e0efb1e05950a5f2d2e86e67adcbf547f8)
                To use the PC official images, refer to the [planetary-computer-containers](https://github.com/Microsoft/planetary-computer-containers) repo.
            gpu: Whether to use GPU workers. Defaults to False.
            environment: Environment variables to set on the workers. Defaults to the GDAL and PYGEOS-related variables set in the PC Hub.
            gateway_cluster_kwargs: Additional keyword arguments to pass to [`dask_gateway.GatewayCluster`](https://gateway.dask.org/api-client.html#dask_gateway.GatewayCluster) constructor.

        Returns:
            A client for the Dask cluster just created.

        Example:
            Instantiate a new cluster using PC Dask Gateway Server:
            ```python
            import dask.array as da
            from prefect import flow
            from prefect_planetary_computer import PlanetaryComputerCredentials

            @flow
            def example_new_dask_gateway_cluster_flow():
                pc_credentials_block = PlanetaryComputerCredentials.load("BLOCK_NAME")

                # Create a Dask Gateway cluster with default configuration
                # it will be automatically used for any subsequent dask compute
                cluster = pc_credentials_block.new_dask_gateway_cluster()

                # Scale the cluster to at most 10 workers
                cluster.adapt(minimum=2, maximum=10)

                # Create a Dask array with 1 billion elements and sum them
                x = da.random.random(1000000000)
                result = x.sum().compute()

                return result

            example_new_dask_gateway_cluster_flow()
            ```
        """  # noqa E501

        gateway = self.get_dask_gateway(**gateway_cluster_kwargs)

        if worker_cores is not None:
            gateway_cluster_kwargs["worker_cores"] = worker_cores
        if worker_memory is not None:
            gateway_cluster_kwargs["worker_memory"] = worker_memory
        if image is not None:
            gateway_cluster_kwargs["image"] = image
        if gpu is not None:
            gateway_cluster_kwargs["gpu"] = gpu
        if environment is not None:
            gateway_cluster_kwargs["environment"] = environment

        return gateway.new_cluster(**gateway_cluster_kwargs)