from pydantic import BaseModel, Field


class QueryArynDocSetModel(BaseModel):
    """
    Input schema for query_aryn_docset()

    Attributes:
        docset_id (str, required)
        query (str, required)
        summarize_result (bool, optional)
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The id of the docset to query.""",
    )

    query: str = Field(
        ...,
        description="""
            query (str, required)
            The query to search for in the docset.  """,
    )

    summarize_result: bool = Field(
        False,
        description="""
            summarize_result (bool, optional)
            Whether to summarize the result of the query.""",
    )
