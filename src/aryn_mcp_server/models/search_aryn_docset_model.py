from pydantic import BaseModel, Field, model_validator
from typing import Literal
from .properties_filter_model import PropertiesFilterModel


class SearchArynDocSetModel(BaseModel):
    """
    Input schema for search_aryn_docset()

    Attributes:
        docset_id
        query
        query_type
        properties_filter
        k
        return_type
        page_token
    """

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The unique identifier of the DocSet you are searching over""",
    )

    query_or_properties_filter: Literal["query", "properties_filter"] = Field(
        ...,
        description="""
            query_or_properties_filter (str, required)
            An enum that can be either 2 values:
            query:      When query is specified, the search call will use the query parameter to search over the docset.
            properties_filter:  When properties_filter is specified, the search call will use the properties to search over the docset.""",
    )

    query: str | None = Field(
        None,
        description="""
            query (str | None, optional)
            A string that specifies what term you are searching for within the contents of your documents.
            The query_type parameter specified will control exactly how this query parameter is used
            to search over your documents.""",
    )

    query_type: None | Literal["keyword", "vector", "lexical", "hybrid"] = Field(
        "lexical",
        description="""
            query_type (str, required)
            An enum that can be either 4 values:
            keyword:  When keyword is specified, the search call will perform a substring match and return
                      results that contain strings that contain the query term specified.
            vector:   When vector is specified, the search call will internally embed the query with the
                      embedding function associated with the docset you are querying on and perform a k-nearest
                      neighbor search to retrieve the results.
            lexical:  When lexical is specified, the search call will perform an exact string match and return
                      results where the query string shows up as a standalone word.
            hybrid:   A mix of vector and lexical""",
    )

    properties_filter: list[PropertiesFilterModel] | None = Field(
        None,
        description="""
            properties_filter (str | None, optional)
            A list of schemas that help create an expression that specifies the condition to use when extract
            extracting documents or elements from the docset. Default value is None""",
    )

    page_size: int = Field(
        ...,
        description="""
            page_size (int, required)
            The number of records to return back.""",
    )

    return_type: Literal["doc", "element"] = Field(
        ...,
        description="""
            return_type (str, required)
            An enum that an be either 2 values:
            doc:      When doc is specified, documents that match the search criteria are retuned.
            element:  When element is specified, specific sections of the document (i.e. elements) are returned.""",
    )

    page_token: str | None = Field(
        None,
        description="""
            page_token (str | None, optional)
            A string used for pagination purposes. If provided, this will indicate to the server where to begin the
            next set of records to return. Valid for 24 hours.""",
    )

    @model_validator(mode="after")
    def validate_search_criteria(self) -> "SearchArynDocSetModel":
        if self.query_or_properties_filter == "query" and not (self.query or self.query_type):
            raise ValueError("If query_or_properties_filter is 'query', query and query_type must be provided")
        if self.query_or_properties_filter == "properties_filter" and (not self.properties_filter):
            raise ValueError("If query_or_properties_filter is 'properties_filter', properties_filter must be provided")
        return self
