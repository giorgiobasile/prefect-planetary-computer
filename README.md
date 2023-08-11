# prefect-planetary-computer

<p align="center">
    <!--- Insert a cover image here -->
    <!--- <br> -->
    <a href="https://pypi.python.org/pypi/prefect-planetary-computer/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-planetary-computer?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/giorgiobasile/prefect-planetary-computer/" alt="Stars">
        <img src="https://img.shields.io/github/stars/giorgiobasile/prefect-planetary-computer?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-planetary-computer/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-planetary-computer?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/giorgiobasile/prefect-planetary-computer/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/giorgiobasile/prefect-planetary-computer?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

Visit the full docs [here](https://giorgiobasile.github.io/prefect-planetary-computer) to see additional examples and the API reference.

Prefect integrations with the Microsoft [Planetary Computer](https://planetarycomputer.microsoft.com/) (PC).

<a href="https://planetarycomputer.microsoft.com/"><img src="https://camo.githubusercontent.com/55810ac9ab5a7f4bb66d532d6c6afd26ef926a0c2436d506a91bb439a4983194/68747470733a2f2f6169346564617461736574737075626c69636173736574732e626c6f622e636f72652e77696e646f77732e6e65742f6173736574732f616f645f696d616765732f706c616e65746172795f636f6d70757465725f6865616465725f383030772e706e67" class="pc-banner" height=100/></a>

## Overview

This collection includes a [Credentials Block 🔑](https://github.com/giorgiobasile/prefect-planetary-computer/) to store and retrieve a PC subscription key and Jupyter Hub token, with convenience methods to easily interact with the PC [Data Catalog 🌍](https://planetarycomputer.microsoft.com/catalog) and [Dask Gateway 🚀](https://planetarycomputer.microsoft.com/docs/quickstarts/scale-with-dask/) server.

For more information about:

- using Azure services with Prefect and the Planetary Computer, check out the [`prefect-azure`](https://github.com/PrefectHQ/prefect-azure/) collection.
- the integration between Prefect and Dask, check out the [`prefect-dask`](https://github.com/PrefectHQ/prefect-dask/) collection.
- taking advantage of the Planetary Computer data catalog and compute resources, check out the [Planetary Computer documentation](https://planetarycomputer.microsoft.com/docs/).

## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Installation

Install `prefect-planetary-computer` with `pip`:

```bash
pip install prefect-planetary-computer
```

Requires an installation of Python 3.8+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Usage

!!! note
    * The following Examples are adapted from [Planetary Computer - Scale with Dask](https://planetarycomputer.microsoft.com/docs/quickstarts/scale-with-dask/).

    - Require the following additional packages:
        ```
        pip install xarray zarr adlfs netcdf4 prefect_azure
        ```
    - Make sure to share the same python dependencies - in particular `dask` and `distributed` - between the flow execution environment, the Dask Scheduler and Workers, [as explained in the Dask docs](https://docs.dask.org/en/stable/deployment-considerations.html#consistent-software-environments).

#### Computing Dask Collections

Dask collection computations, such as Dask DataFrames, can be supported from within a Prefect task by creating a Dask Gateway cluster using the credentials block within the main flow or task itself.


```python
# Prefect tasks are executed using the default ConcurrentTaskRunner
# Dask Collections tasks are executed on a new temporary Dask cluster 

import xarray as xr

from prefect import flow, task, get_run_logger
from prefect_planetary_computer import PlanetaryComputerCredentials

from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_upload

pc_credentials = PlanetaryComputerCredentials.load("PC_BLOCK_NAME")
bs_credentials = AzureBlobStorageCredentials.load("BS_BLOCK_NAME")

@task
def compute_mean(asset):
    logger = get_run_logger()

    with pc_credentials.new_gateway_cluster(
        name="test-cluster",
        image="pangeo/pangeo-notebook:latest"
    ) as cluster:

        cluster.adapt(minimum=2, maximum=10)
        client = cluster.get_client()

        ds = xr.open_zarr(
            asset.href,
            **asset.extra_fields["xarray:open_kwargs"],
            storage_options=asset.extra_fields["xarray:storage_options"]
        )
        logger.info(f"Daymet dataset info\n: {ds}")
    
        timeseries = ds["tmin"].mean(dim=["x", "y"]).compute()
        logger.info(f"Mean timeseries info\n: {timeseries}")

    return timeseries

@flow
def pc_dask_flow():

    # get a configured PySTAC client
    catalog = pc_credentials.get_stac_catalog()

    # compute the minimum daily temperature averaged over all of Hawaii, 
    # using the Daymet dataset
    asset = catalog.get_collection("daymet-daily-hi").assets["zarr-abfs"]
    prefect_future = compute_mean.submit(asset)
    timeseries = prefect_future.result()

    # save NetCDF timeseries file
    timeseries.to_netcdf("timeseries.nc")

    # upload to 'my-container' blob storage container
    with open("timeseries.nc", "rb") as f:
        blob = blob_storage_upload(
            data=f.read(),
            container="my-container",
            blob="timeseries.nc",
            blob_storage_credentials=bs_credentials,
            overwrite=False,
        )

    # return the blob name of the uploaded timeseries object
    return blob

pc_dask_flow()
```

#### Using the Dask Task Runner

Prefect's [`prefect_dask.DaskTaskRunner`](https://prefecthq.github.io/prefect-dask/task_runners/#prefect_dask.task_runners.DaskTaskRunner) automatically instatiates a temporary Dask cluster at flow execution time, enabling submission of both Prefect and Dask Collections tasks.

!!! warning
    - [`prefect-dask`](https://prefecthq.github.io/prefect-dask/) requires:
        ```
        distributed==2022.2.0; python_version < '3.8'
        distributed>=2022.5.0,<=2023.3.1
        ```
    - It requires [less configuration](https://discourse.prefect.io/t/how-can-i-resolve-this-error-prefecthttpstatuserror-on-prefect-2-3-1-dasktaskrunner/1541/4) on the Dask Workers side when using Prefect Cloud, you can [get started](https://docs.prefect.io/2.11.3/cloud/cloud-quickstart/) for free.

```python
# Both Prefect tasks and Dask Collections task are executed
# on a new temporary Dask cluster 
import xarray as xr

from prefect import flow, task, get_run_logger
from prefect_planetary_computer import PlanetaryComputerCredentials

from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_upload

from prefect_dask import get_dask_client 

pc_credentials = PlanetaryComputerCredentials.load("PC_BLOCK_NAME")
bs_credentials = AzureBlobStorageCredentials.load("BS_BLOCK_NAME")

pc_runner = pc_credentials.get_dask_task_runner(
    cluster_kwargs={
        "image": "pangeo/pangeo-notebook:latest",
    },
    adapt_kwargs={'minimum': 1, 'maximum': 10, 'active': True}
)

@task
def compute_mean(asset):
    logger = get_run_logger()

    with get_dask_client() as client:
        ds = xr.open_zarr(
            asset.hr
            **asset.extra_fields["xarray:open_kwargs"],
            storage_options=asset.extra_fields["xarray:storage_options"]
        )
        logger.info(f"Daymet dataset info\n: {ds}")

        timeseries = ds["tmin"].mean(dim=["x", "y"]).compute()
        logger.info(f"Mean timeseries info\n: {timeseries}")

    return timeseries

@flow(task_runner=pc_runner)
def pc_dask_flow():
    
    # get a configured PySTAC client
    catalog = pc_credentials.get_stac_catalog()

    # compute the minimum daily temperature averaged over all of Hawaii, 
    # using the Daymet dataset
    asset = catalog.get_collection("daymet-daily-hi").assets["zarr-abfs"]

    mean_task = compute_mean.submit(asset)
    timeseries = mean_task.result()

    # save NetCDF timeseries file
    timeseries.to_netcdf("timeseries.nc")

    # upload to 'my-container' blob storage container
    with open("timeseries.nc", "rb") as f:
        blob = blob_storage_upload(
            data=f.read(),
            container="my-container",
            blob="timeseries.nc",
            blob_storage_credentials=bs_credentials,
            overwrite=False,
        )

    # return the blob name of the uploaded timeseries object
    return blob

pc_dask_flow()
```

### Feedback

If you encounter any bugs while using `prefect-planetary-computer`, feel free to open an issue in the [prefect-planetary-computer](https://github.com/giorgiobasile/prefect-planetary-computer) repository.

If you have any questions or issues while using `prefect-planetary-computer`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-planetary-computer`](https://github.com/giorgiobasile/prefect-planetary-computer) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-planetary-computer`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/giorgiobasile/prefect-planetary-computer/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
