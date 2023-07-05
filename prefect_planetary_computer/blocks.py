"""This is an example blocks module"""

from prefect.blocks.core import Block
from pydantic import Field


class PlanetarycomputerBlock(Block):
    """
    A sample block that holds a value.

    Attributes:
        value (str): The value to store.

    Example:
        Load a stored value:
        ```python
        from prefect_planetary_computer import PlanetarycomputerBlock
        block = PlanetarycomputerBlock.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "planetary computer"
    # replace this with a relevant logo; defaults to Prefect logo
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/08yCE6xpJMX9Kjl5VArDS/c2ede674c20f90b9b6edeab71feffac9/prefect-200x200.png?h=250"  # noqa
    _documentation_url = "https://giorgiobasile.github.io/prefect-planetary-computer/blocks/#prefect-planetary-computer.blocks.PlanetarycomputerBlock"  # noqa

    value: str = Field("The default value", description="The value to store.")

    @classmethod
    def seed_value_for_example(cls):
        """
        Seeds the field, value, so the block can be loaded.
        """
        block = cls(value="A sample value")
        block.save("sample-block", overwrite=True)
