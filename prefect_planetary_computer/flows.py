"""This is an example flows module"""
from prefect import flow

from prefect_planetary_computer.blocks import PlanetarycomputerBlock
from prefect_planetary_computer.tasks import (
    goodbye_prefect_planetary_computer,
    hello_prefect_planetary_computer,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    PlanetarycomputerBlock.seed_value_for_example()
    block = PlanetarycomputerBlock.load("sample-block")

    print(hello_prefect_planetary_computer())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_planetary_computer())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
