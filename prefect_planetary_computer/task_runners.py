"""
Interface and implementations of the Planetary Computer Task Runner, 
inheriting from [`prefect_dask.DaskTaskRunner`](https://prefecthq.github.io/prefect-dask/task_runners/#prefect_dask.task_runners.DaskTaskRunner).
"""  # noqa E501

from dask_gateway.auth import JupyterHubAuth
from prefect_dask import DaskTaskRunner

from prefect_planetary_computer import PlanetaryComputerCredentials
from prefect_planetary_computer.constants import GATEWAY_ADDRESS, GATEWAY_PROXY_ADDRESS


class PlanetaryComputerTaskRunner(DaskTaskRunner):
    """
    A parallel task runner based on
    [`prefect_dask.DaskTaskRunner`](https://prefecthq.github.io/prefect-dask/task_runners/#prefect_dask.task_runners.DaskTaskRunner),
    providing PC-specific configuration.

    It uses [`dask_gateway.GatewayCluster`](https://gateway.dask.org/api-client.html#gatewaycluster)
    to create a Dask cluster on the Planetary Computer, enabling submission of both Prefect
    and Dask Collections tasks to the cluster.

    !!! warning "Multiprocessing safety"
    Note that, because the `PlanetaryComputerTaskRunner` uses multiprocessing, calls to flows
    in scripts must be guarded with `if __name__ == "__main__":` or warnings will
    be displayed.

    Args:
        cluster_kwargs: Additional kwargs to pass to
            [`dask_gateway.GatewayCluster`](https://gateway.dask.org/api-client.html#gatewaycluster)
            when creating a temporary dask cluster.
        adapt_kwargs: Additional kwargs to pass to
            [`dask_gateway.Gateway,adapt_cluster`](https://gateway.dask.org/api-client.html#dask_gateway.Gateway.adapt_cluster)
            when creating a temporary cluster.
            Note that adaptive scaling is only enabled if `adapt_kwargs` are provided.
        client_kwargs (dict, optional): Additional kwargs to use when creating a
            [`dask.distributed.Client`](https://distributed.dask.org/en/latest/api.html#client).

    Examples:
        Using a temporary PC Dask Gateway cluster:
        ```python
        from prefect import flow
        from prefect_planetary_computer.task_runners import PlanetaryComputerTaskRunner

        pc_credentials = PlanetaryComputerCredentials.load("BLOCK_NAME")

        pc_runner = PlanetaryComputerTaskRunner(
            credentials=pc_credentials
        )

        @flow(task_runner=pc_runner)
        def my_flow():
            ...
        ```

        Providing additional kwargs to the PC Dask Gateway cluster:
        ```python
        PlanetaryComputerTaskRunner(
            credentials=PlanetaryComputerCredentials.load("BLOCK_NAME"),
            cluster_kwargs={
                "image": "mcr.microsoft.com/planetary-computer/python:latest",
            },
            adapt_kwargs={'minimum': 1, 'maximum': 10, 'active': True}
        )
        ```

        Connecting to an existing PC `GatewayCluster` (use the base `DaskTaskRunner` for this):
        ```python
        DaskTaskRunner(
            address=cluster.address,
            client_kwargs={'security': cluster.security}
        )
        ```

    """  # noqa: E501

    def __init__(
        self,
        credentials: PlanetaryComputerCredentials,
        cluster_kwargs: dict = None,
        adapt_kwargs: dict = None,
        client_kwargs: dict = None,
    ):
        self.credentials = credentials
        self.hub_api_token = credentials.hub_api_token.get_secret_value()

        if self.hub_api_token is None:
            raise ValueError("JupyterHub API Token hasn't been provided.")

        if cluster_kwargs is None:
            cluster_kwargs = {}

        cluster_kwargs.update(
            {
                "address": GATEWAY_ADDRESS,
                "proxy_address": GATEWAY_PROXY_ADDRESS,
                "auth": JupyterHubAuth(api_token=self.hub_api_token),
            }
        )

        super().__init__(
            address=None,
            cluster_class="dask_gateway.GatewayCluster",
            cluster_kwargs=cluster_kwargs,
            adapt_kwargs=adapt_kwargs,
            client_kwargs=client_kwargs,
        )

    def duplicate(self):
        """
        Return a new task runner instance with the same options.
        Overrides `prefect.task_runners.BaseTaskRunner`.
        """
        return type(self)(
            credentials=self.credentials,
            cluster_kwargs=self.cluster_kwargs,
            adapt_kwargs=self.adapt_kwargs,
            client_kwargs=self.client_kwargs,
        )
