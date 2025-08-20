from pydantic import BaseModel, Field


class GetArynDocSetModel(BaseModel):
    """
    Input schema for get_aryn_docset()

    Attributes:
        docset_id
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet to retrieve""",
    )
