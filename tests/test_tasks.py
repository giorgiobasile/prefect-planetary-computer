from prefect import flow

from prefect_planetary_computer.tasks import (
    goodbye_prefect_planetary_computer,
    hello_prefect_planetary_computer,
)


def test_hello_prefect_planetary_computer():
    @flow
    def test_flow():
        return hello_prefect_planetary_computer()

    result = test_flow()
    assert result == "Hello, prefect-planetary-computer!"


def goodbye_hello_prefect_planetary_computer():
    @flow
    def test_flow():
        return goodbye_prefect_planetary_computer()

    result = test_flow()
    assert result == "Goodbye, prefect-planetary-computer!"
