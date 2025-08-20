from pydantic import BaseModel, Field


class DeleteArynDocumentModel(BaseModel):
    """
    Input schema for delete_aryn_document()

    Attributes:
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
            The unique identifier of the document to delete""",
    )
