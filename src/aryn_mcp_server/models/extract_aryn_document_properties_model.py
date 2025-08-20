from pydantic import BaseModel, Field
from .document_schema import Schema


class ExtractArynDocumentPropertiesModel(BaseModel):
    """
    Input schema for extract_aryn_document_properties()

    Attributes:
        docset_id
        schema
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet containing the documents""",
    )

    document_schema: Schema | None = Field(
        None,
        description="""
            schema (Schema, optional)
            A schema that defines properties to extract out of the document being parsed. For example,
            In a DocSet of quarterly reports, a property to extract would be the date, or the quarter.""",
    )
