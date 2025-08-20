from pydantic import BaseModel, Field
from typing import Literal


class GetArynDocumentExtractedPropertiesModel(BaseModel):
    """
    Input schema for get_aryn_document()

    Attributes:
        docset_id
        doc_id
        output_format
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet containing the document""",
    )

    doc_id: str = Field(
        ...,
        description="""
            doc_id (str, required)
            The unique identifier of the document to retrieve""",
    )

    output_format: Literal["json", "csv"] = Field(
        ...,
        description="""
            output_format (str, required)
            The format to save the extracted properties in. There are two possible values:
            json
            csv""",
    )
