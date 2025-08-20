from pydantic import BaseModel, Field
from .document_schema import Schema


class CreateArynDocSetModel(BaseModel):
    """
    Input schema for create_aryn_docset()

    Attributes:
        name
        schema
    """

    name: str = Field(
        ...,
        description="""
            name (str, required)
            The name of the Aryn DocSet""",
    )

    document_schema: Schema | None = Field(
        None,
        description="""
            schema (Schema, optional)
            A schema that defines properties to extract out of a document being parsed. For example,
            In a DocSet of quarterly reports, a property to extract would be the date, or the quarter.""",
    )
