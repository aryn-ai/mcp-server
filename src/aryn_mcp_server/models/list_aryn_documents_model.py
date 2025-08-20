from pydantic import BaseModel, Field


class ListArynDocumentsModel(BaseModel):
    """
    Input schema for list_aryn_documents()

    Attributes:
        docset_id
        page_size
        page_token
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            ID of the DocSet containing the documents""",
    )

    page_size: int = Field(
        100,
        description="""
            page_size (int, optional)
            Number of items per page""",
    )

    page_token: str | None = Field(
        None,
        description="""
            page_token (str, optional)
            Token for pagination. If provided, this will indicate to the server where to begin the next set
            of records to return. Valid for 24 hours.""",
    )
