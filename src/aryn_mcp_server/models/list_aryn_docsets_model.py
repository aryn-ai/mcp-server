from pydantic import BaseModel, Field


class ListArynDocSetsModel(BaseModel):
    """
    Input schema for list_aryn_docsets()

    Attributes:
        page_size
        name_eq
    """

    page_size: int = Field(
        100,
        description="""
            page_size (int, optional)
            the number of items per page. Default value is 100""",
    )

    name_eq: str | None = Field(
        None,
        description="""
            name_eq (str, optional)
            When provided, filters DocSets by exact name match. Value should be None if no filter is to be done.""",
    )

    page_token: str | None = Field(
        None,
        description="""
            page_token (str, optional)
            Token for pagination. If provided, this will indicate to the server where to begin the next set
            of records to return. Valid for 24 hours.""",
    )
