from pydantic import BaseModel, Field
from typing import Literal, Union, Optional, Any
from os import PathLike


class PartitionModel(BaseModel):
    """
    Input schema for PartitionModel

    Attributes:
        filename: Name of the file to save (no extension)
        file: Path to document or http link to publicly accessible document
        threshold: Cutoff for detecting bounding boxes (float 0.0-1.0 or "auto")
        text_mode: Text extraction mode ("auto", "inline_fallback_to_ocr", "ocr_standard", "ocr_vision")
        table_mode: Table structure extraction mode ("none", "standard", "vision", "custom")
        remove_line_breaks: Whether to remove line breaks from text
        include_additional_text: Enhance table structure by merging text extraction tokens
        extract_images: Whether to extract images from document
        extract_image_format: Format for extracted images ("PPM", "PNG", "JPEG") - deprecated
        summarize_images: Generate text summary of images
        selected_pages: List of pages/page ranges to partition
        strategy: Chunking strategy ("context_rich", "maximize_within_limit")
        max_tokens: Token cutoff for splitting chunks
        tokenizer: Tokenizer type ("openai_tokenizer", "character_tokenizer", "huggingface_tokenizer")
        merge_across_pages: Whether to merge chunks across page boundaries
        output_format: Output representation ("markdown", "html", "json")
        promote_title: Whether to promote element to title if none exists
        title_candidate_elements: List of element types that can be promoted to title
        orientation_correction: Whether to correct page orientation
        include_pagenum: Include page numbers in markdown output
        include_headers: Include headers in markdown output
        include_footers: Include footers in markdown output
        aryn_api_key: Aryn API key for authentication
        region: Aryn region to use ("US", "EU")
        ocr_language: Language for OCR processing
        text_extraction_options: Dictionary of text extraction options
        table_extraction_options: Dictionary of table extraction options
        image_extraction_options: Dictionary of image extraction options
        chunking_options: Dictionary of chunking options
        markdown_options: Dictionary of markdown formatting options
        output_label_options: Dictionary of output label processing options
        return_pdf_base64: Return PDF as base64 encoded string
        ssl_verify: Whether to verify SSL certificates
        docparse_url: URL of Aryn DocParse endpoint
        content_type: MIME type of uploaded file
        add_to_docset_id: ID of Aryn DocSet to add document to
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

    threshold: Optional[Union[float, Literal["auto"]]] = Field(
        None,
        description="""
            threshold (float | "auto", optional)
            A float between 0.0 and 1.0, inclusive, which serves as a cutoff to determine which bounding boxes for
            the file are returned or a string "auto" (the default) where the service uses a processing method to find the best prediction for each
            possible bounding box. Only bounding boxes that the model predicts with a confidence score higher than the threshold specified will
            be returned. A lower value will include more objects but may have overlaps, while a higher value will reduce the number of overlaps
            but may miss legitimate objects.

            If you do set the threshold manually, I recommend starting with a value of 0.32. If not specified, the default value is 'auto'.""",
    )

    text_mode: Literal["auto", "inline_fallback_to_ocr", "ocr_standard", "ocr_vision"] = Field(
        "auto",
        description="""
            text_mode (str, optional)
            A string that specifies the mode to use for text extraction. There are 4 possible values:
            auto:                   Automatically selects the best mode based on the file.
            inline_fallback_to_ocr:                 Tries to extract the embedded text elementwise and falls back to performing OCR otherwise.
            ocr_standard:           Uses the classical OCR pipeline
            ocr_vision:             Uses a vision model for OCR. Note that ocr_vision is only available for PAYG users only.""",
    )

    table_mode: Optional[Literal["none", "standard", "vision", "custom"]] = Field(
        "standard",
        description="""
            table_mode (str | None, optional)
            A string that specifies the mode to use for table structure extraction. There are 4 possible values:
            none:      will not extract table structure
            standard:  will use the standard hybrid table structure extraction pipeline. This is the default value
            vision:    will use a vision model to extract table structure
            custom:    will use the custom expression described by the model_selection parameter in the table_extraction_options""",
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

    extract_image_format: Optional[Literal["PPM", "PNG", "JPEG"]] = Field(
        None,
        description="""
            extract_image_format (str, optional)
            A string indicating what in what format extracted images should be returned. In all cases, the result will be base64
            encoded before being returned. There are 3 possible values:
            PPM: This is the default
            PNG
            JPEG
            Note: This parameter is deprecated in favor of image_extraction_options.""",
    )

    summarize_images: bool = Field(
        False,
        description="""
            summarize_images (bool, optional)
            A boolean that, when True, generates a summary of the images in the document and returns it as the text_representation.
            When False, images are not summarized. Default is False.""",
    )

    selected_pages: Optional[list[Union[list[int], int]]] = Field(
        None,
        description="""
            selected_pages (list[Union[list[int], int]], optional)
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

    tokenizer: Literal["openai_tokenizer", "character_tokenizer", "huggingface_tokenizer"] = Field(
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

    output_format: Optional[Literal["markdown", "html", "json"]] = Field(
        None,
        description="""
            output_format (str, optional)
            Controls output representation; can be set to:
            "markdown"
            "html"
            "json" (default)""",
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

    region: Optional[Literal["US", "EU"]] = Field(
        None,
        description="""
            region (str, optional)
            Specify the Aryn region to use. Valid options are "US" and "EU".""",
    )

    ocr_language: Optional[str] = Field(
        None,
        description="""
            ocr_language (str, optional)
            specify the language to use for OCR. If not set, the language will be english. Default: English""",
    )

    text_extraction_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            text_extraction_options (dict, optional)
            Specify options for text extraction, supports 'ocr_text_mode', with valid options 'vision' and 'standard' and boolean
            'remove_line_breaks'. For 'ocr_text_mode', attempt to extract all non-table text
            using vision models if 'vision', else will use the standard OCR pipeline. Vision is useful for documents with complex layouts
            or non-standard fonts. 'remove_line_breaks' will remove line breaks from the text.
            default: {'remove_line_breaks': True}""",
    )

    table_extraction_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            table_extraction_options (dict, optional)
            Specify options for table extraction. Only enabled if table extraction
            is enabled. Default is {}. Options:
            - 'include_additional_text': Attempt to enhance the table structure by merging in tokens from
                text extraction. This can be useful for tables with missing or misaligned text. Default: False
            - 'model_selection': expression to instruct DocParse how to choose which model to use for table
                extraction. See https://docs.aryn.ai/docparse/processing_options for more details. Default:
                "pixels > 500 -> deformable_detr; table_transformer""",
    )

    image_extraction_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            image_extraction_options (dict, optional)
            Specify options for image extraction. Only enabled if image extraction
            is enabled. Default is {}. Options:
            - 'associate_captions': associate captions with the images they describe. Returns the resized image with the caption
                as a caption attribute. Default: False
            - 'extract_image_format': specify the format of the extracted images. Only applies when extract_images=True.
              Must be one of ["PPM", "PNG", "JPEG"]
            default: "PPM""",
    )

    chunking_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            chunking_options (dict, optional)
            Specify options for chunking the document.
            You can use the default the chunking options by setting this to {}.
            Here is an example set of chunking options:
            {
                'strategy': 'context_rich',
                'tokenizer': 'openai_tokenizer',
                'tokenizer_options': {'model_name': 'text-embedding-3-small'},
                'max_tokens': 512,
                'merge_across_pages': True
            }
            default: None""",
    )

    markdown_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            markdown_options (dict, optional)
            A dictionary for configuring markdown output behavior. It supports three options:
            include_headers, a boolean specifying whether to include headers in the markdown output, include_footers,
            a boolean specifying whether to include footers in the markdown output, and include_pagenum, a boolean
            specifying whether to include page numbers in the markdown output. Here is an example set of markdown
            options:
                {
                    "include_headers": True,
                    "include_footers": True,
                    "include_pagenum": True
                }""",
    )

    output_label_options: Optional[dict[str, Any]] = Field(
        None,
        description="""
            output_label_options (dict, optional)
            A dictionary for configuring output label behavior. It supports three options:
            promote_title, a boolean specifying whether to pick the largest element by font size on the first page
                from among the elements on that page that have one of the types specified in title_candidate_elements
                and promote it to type "Title" if there is no element on the first page of type "Title" already.
            title_candidate_elements, a list of strings representing the label types allowed to be promoted to
                a title.
            orientation_correction, a boolean specifying whether to pagewise rotate pages to the correct orientation
                based off the orientation of text. Pages are rotated by increments of 90 degrees to correct their
                orientation.
            Here is an example set of output label options:
                {
                    "promote_title": True,
                    "title_candidate_elements": ["Section-header", "Caption"],
                    "orientation_correction": True
                }
            default: None (no element is promoted to "Title")""",
    )

    add_to_docset_id: str | None = Field(
        None,
        description="""
            add_to_docset_id (str | None, optional)
            The id of the Aryn DocSet the partitioned file will get added to. Default value is None. """,
    )
