from pydantic import BaseModel, Field


class DeleteArynDocSetModel(BaseModel):
    """
    Input schema for delete_aryn_docset()

    Attributes:
        docset_id
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet to delete""",
    )
