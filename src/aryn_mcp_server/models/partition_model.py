from pydantic import BaseModel, Field
from typing import Literal
from os import PathLike


class PartitionModel(BaseModel):
    """
    Input schema for PartitionModel

    Attributes:
        filename
        file
        threshold
        text_mode
        table_mode
        remove_line_breaks
        include_additional_text
        model_selection
        extract_images
        extract_image_format
        summarize_images
        selected_pages
        strategy
        max_tokens
        tokenizer
        merge_across_pages
        output_format
        promote_title
        title_candidate_elements
        orientation_correction
        include_pagenum
        include_headers
        include_footers
    """

    filename: str = Field(
        "",
        description="""
            filename (str, required)
            Name of the file to save. No file extension should be included.""",
    )

    file: str | PathLike = Field(
        "",
        description="""
            file (str | PathLike, required)
            Path to the PDF document or an http link to a publicly accessible PDF document""",
    )

    threshold: float = Field(
        0.32,
        description="""
            threshold (float, optional)
            A float between 0.0 and 1.0, inclusive, which serves as a cutoff to determine which bounding boxes for
            the file are returned or a string auto (the default) where the service uses a processing method to find the best prediction for each
            possible bounding box. Only bounding boxes that the model predicts with a confidence score higher than the threshold specified will
            be returned. A lower value will include more objects but may have overlaps, while a higher value will reduce the number of overlaps
            but may miss legitimate objects. If you do set the threshold manually, I recommend starting with a value of 0.32.""",
    )

    text_mode: Literal[
        "inline_fallback_to_ocr", "inline", "ocr_standard", "ocr_vision"
    ] = Field(
        "inline_fallback_to_ocr",
        description="""
            text_mode (str, optional)
            A string that specifies the mode to use for text extraction. There are 4 possible values:
            inline_fallback_to_ocr: Tries to extract the embedded text elementwise and falls back to performing OCR otherwise. This is the default.
            inline:                 Extracts embedded text
            ocr_standard:           Uses the classical OCR pipeline
            ocr_vision:             Uses a vision model for OCR. Note that ocr_vision is only available for PAYG users only.""",
    )

    table_mode: Literal[None, "standard", "vision"] = Field(
        "standard",
        description="""
            table_mode (str | None, optional)
            A string that specifies the mode to use for table structure extraction. There are 3 possible values:
            None:      will not extract table structure
            standard:  will use the standard hybrid table structure extraction pipeline. This is the default value
            vision:    will use a vision model to extract table structure""",
    )

    remove_line_breaks: bool = Field(
        False,
        description="""
            remove_line_breaks (bool, optional)
            A boolean that specifies whether to remove line breaks from the text. Default is False.""",
    )

    include_additional_text: bool = Field(
        False,
        description="""
            include_additional_text (bool, optional)
            A value in a map with string keys specifying options for table extraction. When True, will attempt to enhance the table
            structure by merging in tokens from text extraction. This can be useful for working with tables that have missing or
            misaligned text. Default is False""",
    )

    extract_images: bool = Field(
        False,
        description="""
            extract_images (bool, optional)
            A boolean that determines whether to extract images from the document. Default value is False""",
    )

    extract_image_format: Literal["ppm", "png", "jpeg"] = Field(
        "ppm",
        description="""
            extract_image_format (str, optional)
            A string indicating what in what format extracted images should be returned. In all cases, the result will be base64
            encoded before being returned. There are 3 possible values:
            ppm: This is the default
            png
            jpeg""",
    )

    summarize_images: bool = Field(
        False,
        description="""
            summarize_images (bool, optional)
            A boolean that, when True, generates a summary of the images in the document and returns it as the text_representation.
            When False, images are not summarized. Default is False.""",
    )

    selected_pages: list[int] | None = Field(
        None,
        description="""
            selected_pages (list[int], optional)
            A list specifying individual pages (1-indexed) and page ranges from the document to partition. Single pages are
            specified as integers and ranges are specified as lists with two integer entries in ascending order. A valid example
            value for selected_pages is [1, 10, [15, 20]] which would include pages 1, 10, 15, 16, 17 …, 20. selected_pages is
            None by default, which results in all pages of the document being parsed.""",
    )

    strategy: Literal["context_rich", "maximize_within_limit"] = Field(
        "context_rich",
        description="""
            strategy (str, optional)
            A value in a dictionary of options for specifying chunking behavior. A string specifying the strategy to use to combine and split chunks.
            There are two possible values:
            context_rich:           The goal of this chunking strategy is to add context to evenly-sized chunks. This is most useful for retrieval
                                    based GenAI applications. Context_rich chunking combines adjacent section-header and title elements into a
                                    new section-header element. Merges elements into a chunk with its most recent section-header. If the chunk
                                    would contain too many tokens, then it starts a new chunk copying the section-header to the start of this new
                                    chunk and continues. Merges elements on different pages, unless merge_across_pages is set to False.
            maximize_within_limit:  The goal of the maximize_within_limit chunker is to make the chunks as large as possible. Merges elements into
                                    the last most recently merged set of elements unless doing so would make its token count exceed max_tokens. In
                                    that case, it would keep the new element separate and start merging subsequent elements into that one, following
                                    the same rule. Merges elements on different pages, unless merge_across_pages is set to False.""",
    )

    max_tokens: int = Field(
        512,
        description="""
            max_tokens (int, optional)
            A value in a dictionary of options for specifying chunking behavior. An integer specifying the cutoff for splitting chunks that are too
            large. Default value is 512.""",
    )

    tokenizer: Literal[
        "openai_tokenizer", "character_tokenizer", "huggingface_tokenizer"
    ] = Field(
        "openai_tokenizer",
        description="""
            tokenizer (str, optional)
            A value in a dictionary of options for specifying chunking behavior. A string specifying the tokenizer to use when determining how characters
            in a chunk are grouped. There are 3 possible values:
            openai_tokenizer: This is the default value
            character_tokenizer
            huggingface_tokenizer""",
    )

    merge_across_pages: bool = Field(
        True,
        description="""
            merge_across_pages (bool, optional)
            A value in a dictionary of options for specifying chunking behavior. A boolean that when True the selected chunker will attempt to merge chunks
            across page boundaries. Does not apply to the mixed_multi_column merger, which never merges across pages. Defaults to True.""",
    )

    output_format: Literal["markdown", "json"] = Field(
        "json",
        description="""
            output_format (str, required)
            Can be either two values:
            "markdown"
            "json""",
    )

    promote_title: bool = Field(
        False,
        description="""
            promote_title (bool, optional)
            A value in a dictionary of options to specify which heuristic to apply to enforce certain label outputs. A boolean that specifies whether to
            promote an element to title if there’s no title in the output. Default value is False""",
    )

    title_candidate_elements: list[str] = Field(
        [],
        description="""
            title_candidate_elements (list[str], optional)
            A value in a dictionary of options to specify which heuristic to apply to enforce certain label outputs. A list of strings that are candidate
            elements to be promoted to title. Default value is []""",
    )

    orientation_correction: bool = Field(
        False,
        description="""
            orientation_correction (bool, optional)
            A value in a dictionary of options to specify which heuristic to apply to enforce certain label outputs. A boolean value that specifies whether
            to correct the orientation of rotated pages during the preprocessing step. Default value is False""",
    )

    include_pagenum: bool = Field(
        False,
        description="""
            include_pagenum (bool, optional)
            A value in a dictionary of options to specify what to include in the markdown output. A boolean that specifies whether to include page numbers
            in the markdown output. Default is False.""",
    )

    include_headers: bool = Field(
        False,
        description="""
            include_headers (bool, optional)
            A value in a dictionary of options to specify what to include in the markdown output.  A boolean that specifies whether to include headers
            in the markdown output. Default is False.""",
    )

    include_footers: bool = Field(
        False,
        description="""
            include_footers (bool, optional)
            A value in a dictionary of options to specify what to include in the markdown output. A boolean that specifies whether to include footers
            in the markdown output. Default is False.""",
    )

    add_to_docset_id: str | None = Field(
        None,
        description="""
            add_to_docset_id (str | None, optional)
            The id of the Aryn DocSet the partitioned file will get added to. Default value is None. """,
    )
