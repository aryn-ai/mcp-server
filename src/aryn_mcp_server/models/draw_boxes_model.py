from pydantic import BaseModel, Field, field_validator, model_validator


class PageRange(BaseModel):
    start: int = Field(
        ...,
        description="""
            start (int, required)
            The start page number. The page numbers are 1-indexed.""",
    )
    end: int = Field(
        ...,
        description="""
            end (int, required)
            The end page number. The page numbers are 1-indexed.""",
    )

    @model_validator(mode="after")
    def check_start_is_less_than_end(self) -> "PageRange":
        if self.start >= self.end:
            raise ValueError("Start page number must be less than the end page number")
        return self


class DrawBoxesModel(BaseModel):
    """
    Input schema for get_boxes_drawn_on_pdf()

    Attributes:
        docset_id
        doc_id
        path_to_partitioned_json
        path_to_original_pdf
        pages_to_draw_boxes_on
    """

    docset_id: str | None = Field(
        None,
        description="""
            docset_id (str, optional)
            The unique identifier of the DocSet containing the document""",
    )

    doc_id: str | None = Field(
        None,
        description="""
            doc_id (str, optional)
            The unique identifier of the document to retrieve""",
    )

    path_to_partitioned_json: str | None = Field(
        None,
        description="""
            path_to_partitioned_json (str, optional)
            A path to a partitioned pdf in json form. The partitioned result MUST be a json format.""",
    )

    @field_validator("path_to_partitioned_json")
    @classmethod
    def check_is_json(cls, v: str) -> str:
        if not v.lower().endswith(".json"):
            raise ValueError("File type must be json")
        return v

    path_to_original_pdf: str | None = Field(
        None,
        description="""
            path_to_original_pdf (str, optional)
            A path to the original pdf that was partitioned.""",
    )

    @field_validator("path_to_original_pdf")
    @classmethod
    def check_is_pdf(cls, v: str) -> str:
        if not v.lower().endswith(".pdf"):
            raise ValueError("File type must be pdf")
        return v

    pages_to_draw_boxes_on: list[PageRange] = Field(
        ...,
        description="""
            pages_to_draw_boxes_on (list[PageRange], required)
            A list of page ranges to draw boxes on. Ranges are defined like python slices where the start is inclusive and the end is exclusive.
            The page numbers are 1-indexed.""",
    )

    @model_validator(mode="after")
    def validate_mutually_exclusive_fields(self) -> "DrawBoxesModel":
        """
        Validates that either:
        1. docset_id and doc_id are provided
        2. path_to_partitioned_json and path_to_original_pdf are provided
        """
        has_docset_fields = bool(self.docset_id and self.doc_id)
        has_path_fields = bool(
            self.path_to_partitioned_json and self.path_to_original_pdf
        )

        if not has_docset_fields and not has_path_fields:
            raise ValueError(
                "Either (docset_id, doc_id) OR (path_to_partitioned_json, path_to_original_pdf) must be provided. "
                "Both sets cannot be empty."
            )

        if has_docset_fields and has_path_fields:
            raise ValueError(
                "Cannot provide both (docset_id, doc_id) AND (path_to_partitioned_json, path_to_original_pdf). "
                "Please provide only one set of fields."
            )

        return self
