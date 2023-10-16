"""Module handling Microsoft Planetary Computer credentials"""

from typing import Any, Dict, Optional

import dask_gateway
import planetary_computer
import pystac_client
from dask_gateway.auth import JupyterHubAuth
from prefect.blocks.core import Block
from prefect_dask import DaskTaskRunner
from pydantic import VERSION as PYDANTIC_VERSION

from prefect_planetary_computer.constants import (
    CATALOG_URL,
    GATEWAY_ADDRESS,
    GATEWAY_PROXY_ADDRESS,
)

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, SecretStr
else:
    from pydantic import Field, SecretStr


class PlanetaryComputerCredentials(Block):
    """
    Block used to manage Microsoft Planetary Computer credentials.

    The block stores a [subscription key](https://planetarycomputer.microsoft.com/docs/concepts/sas#when-an-account-is-needed)
    to access the full PC data catalog and a
    [JupyterHub API token](https://planetarycomputer.microsoft.com/docs/concepts/computing/#request-a-token-from-jupyterhub)
    to instantiate clusters through Dask Gateway.

    Args:
        subscription_key (str, optional): A subscription key to access the full PC data catalog.
        hub_api_token (str, optional): The JupyterHub API token to instantiate clusters through Dask Gateway.

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

    def __eq__(self, other: Any) -> bool:
        """
        Equality comparison between two `PlanetaryComputerCredentials` instances.
        """
        if not isinstance(other, PlanetaryComputerCredentials):
            return False
        return (
            self.subscription_key == other.subscription_key
            and self.hub_api_token.get_secret_value()
            == other.hub_api_token.get_secret_value()
        )

    def get_stac_catalog(
        self, sign_inplace: bool = True, **pystac_kwargs: Dict
    ) -> pystac_client.Client:
        """
        Provides a [PySTAC client](https://pystac-client.readthedocs.io/en/stable/api.html#client) for the PC data catalog,
        optionally signing items automatically as they are retrieved.

        For more information about PC signing, refer to the [docs](https://planetarycomputer.microsoft.com/docs/concepts/sas).

        Args:
            sign_inplace: Whether to automatically sign items through the
                [planetary_computer.sign_inplace](https://github.com/microsoft/planetary-computer-sdk-for-python#automatic-signing) modifier.
            pystac_kwargs: Additional keyword arguments to pass to the
                [`pystac_client.Client.open`](https://pystac-client.readthedocs.io/en/stable/api.html#pystac_client.Client.open) method.

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

        modifier = planetary_computer.sign_inplace if sign_inplace else None

        return pystac_client.Client.open(
            CATALOG_URL, modifier=modifier, **pystac_kwargs
        )

    def get_gateway(self, **gateway_kwargs: Dict) -> dask_gateway.Gateway:
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
            def example_get_gateway_flow():
                pc_credentials_block = PlanetaryComputerCredentials(
                    subscription_key = "sub-key",
                    hub_api_token = "hub-token"
                )
                gateway = pc_credentials_block.get_gateway()

                # List available clusters
                clusters = gateway.list_clusters()
                return len(clusters)

            example_get_gateway_flow()
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

    def new_gateway_cluster(
        self,
        worker_cores: Optional[float] = None,
        worker_memory: Optional[float] = None,
        image: Optional[str] = None,
        gpu: Optional[bool] = False,
        environment: Optional[dict] = None,
        **gateway_cluster_kwargs: Dict,
    ) -> dask_gateway.GatewayCluster:
        """
        Instantiate a new cluster from the PC Dask Gateway Server.

        Each argument corresponds to one of the available PC Dask Gateway cluster option.
        PC sets some defaults, but they can be overridden by passing the corresponding argument to this function -
        [see Helm chart](https://github.com/microsoft/planetary-computer-hub/blob/main/helm/chart/config.yaml).

        Args:
            worker_cores: Number of cores per worker, in the 0.1-8 range. Defaults to 1.
            worker_memory: Amount of memory per worker (in GiB) in the 1-64 range. Defaults to 8.
            image: The Docker image to be used for the workers.
                Defaults to [`pangeo/pangeo-notebook:latest`](https://hub.docker.com/layers/pangeo/pangeo-notebook/latest/images/sha256-94e97e24adf14e72c01f18c782b8c4e0efb1e05950a5f2d2e86e67adcbf547f8)
                To use the PC official images, refer to the [`planetary-computer-containers`](https://github.com/Microsoft/planetary-computer-containers) repo.
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
            def example_new_cluster_flow():
                pc_credentials_block = PlanetaryComputerCredentials.load("BLOCK_NAME")

                # Create a Dask Gateway cluster with default configuration
                # it will be automatically used for any subsequent dask compute
                cluster = pc_credentials_block.new_cluster()

                # Scale the cluster to at most 10 workers
                cluster.adapt(minimum=2, maximum=10)

                # Create a Dask array with 1 billion elements and sum them
                x = da.random.random(1000000000)
                result = x.sum().compute()

                return result

            example_new_cluster_flow()
            ```
        """  # noqa E501

        gateway_cluster_kwargs.update(
            self._get_cluster_options_dict(
                worker_cores=worker_cores,
                worker_memory=worker_memory,
                image=image,
                gpu=gpu,
                environment=environment,
            )
        )

        return dask_gateway.GatewayCluster(
            address=GATEWAY_ADDRESS,
            proxy_address=GATEWAY_PROXY_ADDRESS,
            auth=JupyterHubAuth(api_token=self.hub_api_token.get_secret_value()),
            **gateway_cluster_kwargs,
        )

    def get_dask_task_runner(
        self,
        worker_cores: Optional[float] = None,
        worker_memory: Optional[float] = None,
        image: Optional[str] = None,
        gpu: Optional[bool] = False,
        environment: Optional[dict] = None,
        cluster_kwargs: Dict = None,
        adapt_kwargs: Dict = None,
        client_kwargs: Dict = None,
    ) -> DaskTaskRunner:

        """
        Provides a [`prefect_dask.DaskTaskRunner`](https://prefecthq.github.io/prefect-dask/task_runners/#prefect_dask.task_runners.DaskTaskRunner)
        with PC-specific configurations.

        This will use the PC Dask Gateway Server to create a new cluster the same way as
        [`PlanetaryComputerCredentials.new_gateway_cluster`](#new_gateway_cluster) does,
        but it will automatically happen at flow submission time.

        Each argument corresponds to one of the available PC Dask Gateway cluster option.
        PC sets some defaults, but they can be overridden by passing the corresponding argument to this function -
        [see Helm chart](https://github.com/microsoft/planetary-computer-hub/blob/main/helm/chart/config.yaml).

        Args:
            worker_cores: Number of cores per worker, in the 0.1-8 range. Defaults to 1.
            worker_memory: Amount of memory per worker (in GiB) in the 1-64 range. Defaults to 8.
            image: The Docker image to be used for the workers.
                Defaults to [`pangeo/pangeo-notebook:latest`](https://hub.docker.com/layers/pangeo/pangeo-notebook/latest/images/sha256-94e97e24adf14e72c01f18c782b8c4e0efb1e05950a5f2d2e86e67adcbf547f8)
                To use the PC official images, refer to the [`planetary-computer-containers`](https://github.com/Microsoft/planetary-computer-containers) repo.
            gpu: Whether to use GPU workers. Defaults to False.
            environment: Environment variables to set on the workers. Defaults to the GDAL and PYGEOS-related variables set in the PC Hub.
            cluster_kwargs: Additional kwargs to pass to
                [`dask_gateway.GatewayCluster`](https://gateway.dask.org/api-client.html#gatewaycluster)
                when creating a temporary dask cluster.
            adapt_kwargs: Additional kwargs to pass to
                [`dask_gateway.Gateway,adapt_cluster`](https://gateway.dask.org/api-client.html#dask_gateway.Gateway.adapt_cluster)
                when creating a temporary cluster.
                Note that adaptive scaling is only enabled if `adapt_kwargs` are provided.
            client_kwargs: Additional kwargs to use when creating a
                [`dask.distributed.Client`](https://distributed.dask.org/en/latest/api.html#client).

        Examples:
            Using a temporary PC Dask Gateway cluster:
            ```python
            from prefect import flow

            pc_credentials = PlanetaryComputerCredentials.load("BLOCK_NAME")

            pc_runner = pc_credentials.get_dask_task_runner()

            @flow(task_runner=pc_runner)
            def my_flow():
                ...
            ```

            Providing additional kwargs to the PC Dask Gateway cluster:
            ```python
            pc_runner = pc_credentials.get_dask_task_runner(
                cluster_kwargs={
                    "image": "pangeo/pangeo-notebook:latest",
                },
                adapt_kwargs={'minimum': 1, 'maximum': 10, 'active': True}
            )
            ```

            Connecting to an existing PC `GatewayCluster` (use `DaskTaskRunner` directly for this):
            ```python
            DaskTaskRunner(
                address=cluster.address,
                client_kwargs={'security': cluster.security}
            )
            ```

        """  # noqa: E501

        if cluster_kwargs is None:
            cluster_kwargs = {}

        cluster_kwargs.update(
            dict(
                address=GATEWAY_ADDRESS,
                proxy_address=GATEWAY_PROXY_ADDRESS,
                auth=JupyterHubAuth(api_token=self.hub_api_token.get_secret_value()),
            )
        )

        cluster_kwargs.update(
            self._get_cluster_options_dict(
                worker_cores=worker_cores,
                worker_memory=worker_memory,
                image=image,
                gpu=gpu,
                environment=environment,
            )
        )

        return DaskTaskRunner(
            cluster_class="dask_gateway.GatewayCluster",
            cluster_kwargs=cluster_kwargs,
            adapt_kwargs=adapt_kwargs,
            client_kwargs=client_kwargs,
        )

    def _get_cluster_options_dict(
        self,
        worker_cores: Optional[float] = None,
        worker_memory: Optional[float] = None,
        image: Optional[str] = None,
        gpu: Optional[bool] = False,
        environment: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Return a dictionary of cluster options accepted by
        the PC `dask_gateway.GatewayCluster` constructor.
        """

        cluster_options = {}

        if worker_cores is not None:
            cluster_options["worker_cores"] = worker_cores
        if worker_memory is not None:
            cluster_options["worker_memory"] = worker_memory
        if image is not None:
            cluster_options["image"] = image
        if gpu is not None:
            cluster_options["gpu"] = gpu
        if environment is not None:
            cluster_options["environment"] = environment

        return cluster_options
