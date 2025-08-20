from pydantic import BaseModel, Field


class DeleteArynDocSetPropertiesModel(BaseModel):
    """
    Input schema for delete_aryn_docset_properties()

    Attributes:
        docset_id
        schema
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet containing the document""",
    )

    properties_to_delete: list[str] = Field(
        ...,
        description="""
            properties_to_delete (list[str], required)
            A list of names of properties to delete""",
    )
