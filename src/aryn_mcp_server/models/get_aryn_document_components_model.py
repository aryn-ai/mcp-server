from pydantic import BaseModel, Field


class GetArynDocumentComponentsModel(BaseModel):
    """
    Input schema for get_aryn_document_elements(), get_aryn_document_tables(), and get_aryn_document_original_file()

    Attributes:
        return_original_elements
        docset_id
        doc_id
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

    return_original_elements: bool = Field(
        ...,
        description="""
            return_original_elements (bool, required)
            Whether to return the original elements or the parsed elements""",
    )
