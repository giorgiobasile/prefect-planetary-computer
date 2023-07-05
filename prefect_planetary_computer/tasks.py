"""This is an example tasks module"""
from prefect import task


@task
def hello_prefect_planetary_computer() -> str:
    """
    Sample task that says hello!

    Returns:
        A greeting for your collection
    """
    return "Hello, prefect-planetary-computer!"


@task
def goodbye_prefect_planetary_computer() -> str:
    """
    Sample task that says goodbye!

    Returns:
        A farewell for your collection
    """
    return "Goodbye, prefect-planetary-computer!"
