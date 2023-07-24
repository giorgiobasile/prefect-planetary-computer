"""This is an example blocks module"""

import os
from enum import StrEnum
from typing import Optional
from pydantic import Field, SecretStr
from dask_gateway import Gateway, GatewayCluster
from dask_gateway.auth import JupyterHubAuth
from prefect.blocks.core import Block


class PlanetaryComputerCredentials(Block):
    """
    Block used to manage Microsoft Planetary Computer credentials.

    The block stores a [subscription key](https://planetarycomputer.microsoft.com/docs/concepts/sas#when-an-account-is-needed) 
    to access the full PC data catalog and a [JupyterHub API token](https://planetarycomputer.microsoft.com/docs/concepts/computing/#request-a-token-from-jupyterhub) to instantiate clusters through Dask Gateway.

    Example:
        Load stored Planetary Computer credentials:
        ```python
        from prefect_planetary_computer import PlanetaryComputerCredentials
        pc_credentials_block = PlanetaryComputerCredentials.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Planetary Computer Credentials"
    _logo_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Microsoft_logo.svg/512px-Microsoft_logo.svg.png"  # noqa
    _documentation_url = "https://giorgiobasile.github.io/prefect-planetary-computer/blocks/#prefect_planetary_computer.blocks.PlanetarycomputerBlock"  # noqa

    pc_sdk_subscription_key: Optional[SecretStr] = Field(
        default=None,
        description="A subscription key to access the full PC data catalog.",
        title="PC Subscription Key",
    )
    hub_api_token: Optional[SecretStr] = Field(
        default=None,
        description="The JupyterHub API token to instantiate clusters through Dask Gateway.",
        title="JupyterHub API Token",
    )

