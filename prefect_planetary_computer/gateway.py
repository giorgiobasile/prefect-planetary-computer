"""Provides PC-specific implementations of the Dask Gateway client and cluster."""

from typing import Dict, Optional

import dask_gateway
from dask_gateway.auth import JupyterHubAuth

from prefect_planetary_computer.constants import GATEWAY_ADDRESS, GATEWAY_PROXY_ADDRESS


class PlanetaryComputerGateway(dask_gateway.Gateway):
    """
    A client for the Planetary Computer Dask Gateway server.

    Args:
        hub_api_token: A Planetary Computer Hub API token
        gateway_kwargs: Additional keyword arguments to pass
            to the Dask Gateway client.
    """

    def __init__(self, hub_api_token: str, **gateway_kwargs: Dict):
        """
        Create a Planetary Computer Gateway.
        """
        if hub_api_token is None:
            raise ValueError("No Planetary Computer Hub API token provided.")
        if gateway_kwargs.get("address", None):
            raise ValueError("The 'address' kwarg is already set up by default.")
        if gateway_kwargs.get("proxy_address", None):
            raise ValueError("The 'proxy_address' kwarg is already set up by default.")

        return super().__init__(
            address=GATEWAY_ADDRESS,
            proxy_address=GATEWAY_PROXY_ADDRESS,
            auth=JupyterHubAuth(api_token=hub_api_token),
            **gateway_kwargs,
        )


class PlanetaryComputerCluster(dask_gateway.GatewayCluster):

    """
    A Planetary Computer Dask cluster.

    Please make sure to **share the same python dependencies** as the Docker image used for the Dask workers,
    [as explained in the Dask docs](https://docs.dask.org/en/stable/how-to/manage-environments.html#manage-environments).

    Each argument corresponds to one of the available PC Dask Gateway cluster option.
    The server sets some defaults, but they can be overridden by passing the corresponding argument to this function -
    [see Helm chart](https://github.com/microsoft/planetary-computer-hub/blob/main/helm/chart/config.yaml).

    Args:
        hub_api_token: A Planetary Computer Hub API token
        worker_cores: Number of cores per worker, in the 0.1-8 range. Defaults to 1.
        worker_memory: Amount of memory per worker (in GiB) in the 1-64 range. Defaults to 8.
        image: The Docker image to be used for the workers.
            Defaults to the latest PC python image. To use other official images or build your own,
            refer to [`planetary-computer-containers`](https://github.com/Microsoft/planetary-computer-containers).
        gpu: Whether to use GPU workers. Defaults to False.
        environment: Environment variables to set on the workers. Defaults to the GDAL and PYGEOS-related variables set in the PC Hub.
        gateway_cluster_kwargs: Additional keyword arguments to pass to [`dask_gateway.GatewayCluster`](https://gateway.dask.org/api-client.html#dask_gateway.GatewayCluster) constructor.

    """  # noqa E501

    def __init__(
        self,
        hub_api_token: str,
        worker_cores: Optional[float] = None,
        worker_memory: Optional[float] = None,
        image: Optional[str] = None,
        gpu: Optional[bool] = False,
        environment: Optional[dict] = None,
        **gateway_cluster_kwargs: Dict,
    ) -> dask_gateway.GatewayCluster:
        """
        Create a Planetary Computer Dask cluster.
        """
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

        return super().__init__(
            address=GATEWAY_ADDRESS,
            proxy_address=GATEWAY_PROXY_ADDRESS,
            auth=JupyterHubAuth(api_token=hub_api_token),
            **gateway_cluster_kwargs,
        )
