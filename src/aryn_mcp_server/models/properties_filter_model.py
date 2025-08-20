from pydantic import BaseModel, Field
from typing import Literal


class PropertiesFilterModel(BaseModel):
    """
    Schema for defining properties to be filtered

    Attributes:
        property
        value
        operator
    """

    property: str = Field(
        ...,
        description="""
            property (str, required)
            The property to be filtering for in a DocSet""",
    )

    value: str = Field(
        ...,
        description="""
            value (str, required)
            The value of the property to be filtering for in a DocSet""",
    )

    property_type: Literal["str", "int", "bool"] = Field(
        ...,
        description="""
            property_type (str, required)
            The type of the property specified in the DocSet. It MUST match the type of the property in the DocSet. There are 3 possible values:
            str:  A string
            int:  A number
            bool: A boolean
            """,
    )

    operator: Literal["=", ">", "<", ">=", "<=", "<>", "like"] = Field(
        ...,
        description="""
            operator (str, required)
            The comparison operator for the property being filtered. There are 7 possible values:
            like:  Documents or elements where the property contains the value
            =:     Documents or elements where the property is equal to the value
            >:     Documents or elements where the property is greater than the value
            <:     Documents or elements where the property is less than the value
            >=:    Documents or elements where the property is greater than or equal to the value
            <=:    Documents or elements where the property is less than or equal to the value
            <>:    Documents or elements where the property is not the value""",
    )
