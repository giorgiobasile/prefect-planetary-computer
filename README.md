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

Prefect integrations with [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/).

<a href="https://planetarycomputer.microsoft.com/"><img src="https://camo.githubusercontent.com/55810ac9ab5a7f4bb66d532d6c6afd26ef926a0c2436d506a91bb439a4983194/68747470733a2f2f6169346564617461736574737075626c69636173736574732e626c6f622e636f72652e77696e646f77732e6e65742f6173736574732f616f645f696d616765732f706c616e65746172795f636f6d70757465725f6865616465725f383030772e706e67" class="pc-banner" height=100/></a>

## Getting started

This collection provides:

- 🔑 A [credentials block](https://github.com/giorgiobasile/prefect-planetary-computer/) block to store and retrieve a subscription key and a Jupyter Hub token.
- 🌍 A configured [PySTAC client](https://github.com/giorgiobasile/prefect-planetary-computer/credentials/#prefect_planetary_computer.credentials.PlanetaryComputerCredentials.get_stac_catalog) to interact with the Planetary Computer data catalog.
- 💻 A configured [Dask Gateway client](https://github.com/giorgiobasile/prefect-planetary-computer/credentials/#prefect_planetary_computer.credentials.PlanetaryComputerCredentials.get_dask_gateway) to programmatically instantiate new Dask clusters and submit distributed computations.
- 🚀 A [task runner](https://github.com/giorgiobasile/prefect-planetary-computer/task_runners/#prefect_planetary_computer.task_runners.PlanetaryComputerTaskRunner) to automatically instatiate temporary Dask clusters at flow execution time, enabling submission of both Prefect and Dask Collections tasks.

For more information on using Azure services with Prefect and the Microsoft Planetary Computer, check out the [prefect-azure](https://github.com/PrefectHQ/prefect-azure/) collection.

### Reading from the STAC API

???+ note
    Example adapted from [Planetary Computer - Reading Data from the STAC API](https://planetarycomputer.microsoft.com/docs/quickstarts/reading-stac/).

```python

# TODO

```

### Scale with Dask

???+ note
    Example adapted from [Planetary Computer - Scale with Dask](https://planetarycomputer.microsoft.com/docs/quickstarts/scale-with-dask/).

```python

# TODO

```

### Writing outputs to Azure Blob Storage

???+ note
    Example adapted from [Planetary Computer - Writing outputs to Azure Blob Storage](https://planetarycomputer.microsoft.com/docs/quickstarts/storage/).

```python

# TODO

```

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

<!--- ### Saving credentials to block

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://docs.prefect.io/ui/blocks/).

Below is a walkthrough on saving block documents through code.

1. Head over to <SERVICE_URL>.
2. Login to your <SERVICE> account.
3. Click "+ Create new secret key".
4. Copy the generated API key.
5. Create a short script, replacing the placeholders (or do so in the UI).

```python
from prefect_planetary_computer import Block
Block(api_key="API_KEY_PLACEHOLDER").save("BLOCK_NAME_PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:

```python
from prefect_planetary_computer import Block
Block.load("BLOCK_NAME_PLACEHOLDER")
```

!!! info "Registering blocks"

    Register blocks in this module to
    [view and edit them](https://docs.prefect.io/ui/blocks/)
    on Prefect Cloud:

    ```bash
    prefect block register -m prefect_planetary_computer
    ```

A list of available blocks in `prefect-planetary-computer` and their setup instructions can be found [here](https://giorgiobasile.github.io/prefect-planetary-computer/blocks_catalog).

--->

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
