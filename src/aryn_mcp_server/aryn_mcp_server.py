import json
import tempfile
from pathlib import Path

from aryn_sdk.partition import draw_with_boxes, tables_to_pandas, partition_file
from mcp.server.fastmcp import FastMCP
from .aryn_docset_manager import ArynDocSetManager
from .aryn_document_manager import ArynDocumentManager
from .utils.utils import save_file, get_output_dir, create_zip_from_dataframes

from .models import (
    PartitionModel,
    DrawBoxesModel,
    CreateArynDocSetModel,
    GetArynDocSetModel,
    ListArynDocSetsModel,
    DeleteArynDocSetModel,
    AddArynDocumentModel,
    ListArynDocumentsModel,
    GetArynDocumentComponentsModel,
    GetArynDocumentExtractedPropertiesModel,
    DeleteArynDocumentModel,
    ExtractArynDocumentPropertiesModel,
    DeleteArynDocSetPropertiesModel,
    SearchArynDocSetModel,
    QueryArynDocSetModel,
)

mcp = FastMCP(
    name="ArynMCPServer",
)

ADSM = ArynDocSetManager()
ADM = ArynDocumentManager()


@mcp.tool()
def partition_pdf(args: PartitionModel) -> str:
    """Converts a document in PDF format to either JSON or Markdown using Aryn's partitioning service

    Args:
        args: The input arguments defined in the PartitionModel schema. These include:
        filename
        file
        threshold
        text_mode
        table_mode
        remove_line_breaks
        include_additional_text
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

    Returns:
        A string describing where the result is stored and the name of the file
    """
    try:
        partition_result = partition_file(
            args.file,
            threshold=args.threshold,
            text_mode=args.text_mode,
            table_mode=args.table_mode,
            text_extraction_options={"remove_line_breaks": args.remove_line_breaks},
            table_extraction_options={
                "include_additional_text": args.include_additional_text,
            },
            extract_images=args.extract_images,
            extract_image_format=args.extract_image_format,
            summarize_images=args.summarize_images,
            selected_pages=args.selected_pages,
            chunking_options={
                "strategy": args.strategy,
                "max_tokens": args.max_tokens,
                "tokenizer": args.tokenizer,
                "merge_across_pages": args.merge_across_pages,
            },
            output_format=args.output_format,
            output_label_options={
                "promote_title": args.promote_title,
                "title_candidate_elements": args.title_candidate_elements,
                "orientation_correction": args.orientation_correction,
            },
            markdown_options={
                "include_pagenum": args.include_pagenum,
                "include_headers": args.include_headers,
                "include_footers": args.include_footers,
            },
            add_to_docset_id=args.add_to_docset_id,
        )

        save_file(partition_result, args.filename, args.output_format)

        return f"File saved in {get_output_dir()} as {args.filename}.{args.output_format}"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_boxes_drawn_on_pdf(args: DrawBoxesModel) -> dict:
    """Saves a list of images from the partitioned pdf, one for each page, with bounding boxes detected by the partitioner drawn on.

    Args:
        args: The input arguments defined in the DrawBoxesModel schema. These include:
        docset_id
        doc_id
        path_to_partitioned_json
        path_to_original_pdf
        pages_to_draw_boxes_on
    Returns:
        result: A dictionary with the saved image paths and the number of images saved
    """

    try:
        if args.path_to_partitioned_json and args.path_to_original_pdf:
            with open(args.path_to_partitioned_json, "r") as f:
                partition_result = json.load(f)
            original_pdf_path = args.path_to_original_pdf
        else:
            assert args.docset_id and args.doc_id, "docset_id and doc_id are required"
            partition_result = ADM.get_document(
                docset_id=args.docset_id, doc_id=args.doc_id, include_elements=True, include_binary=False
            )
            partition_result = {"elements": partition_result["original_elements"]}
            original_pdf_path = ADM.get_document_binary(
                docset_id=args.docset_id,
                doc_id=args.doc_id,
                file_path=Path(tempfile.gettempdir()) / f"{args.doc_id}.pdf",
            )

        pages = draw_with_boxes(original_pdf_path, partition_result)

        saved_images = []
        for page_range in args.pages_to_draw_boxes_on:
            if page_range.start < 1 or page_range.end > len(pages):
                raise ValueError(f"Page range {page_range} is out of bounds for the document")
            for page_index in range(page_range.start - 1, page_range.end):
                saved_path = save_file(pages[page_index], f"{args.doc_id}_page_image_{page_index+1}", "png")
                saved_images.append(saved_path)

        return {"saved_image_paths": saved_images, "saved_image_count": len(saved_images)}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Group: DocSet Managment Functions
# =============================================================================
@mcp.tool()
def create_aryn_docset(args: CreateArynDocSetModel) -> dict:
    """Creates a new Aryn DocSet to store documents

    Args:
        args: The input arguments defined in the CreateArynDocsetModel schema. These include:
        name
        schema
    Returns:
        result: A DocSetMetadata dictionary
    """

    try:
        docset_info = ADSM.create_docset(name=args.name, schema=args.document_schema)
        return docset_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_aryn_docset_metadata(args: GetArynDocSetModel) -> dict:
    """Gets an Aryn DocSet to store documents

    Args:
        args: The input arguments defined in the GetArynDocsetModel schema. These include:
        docset_id
    Returns:
        result: A DocSetMetadata dictionary
    """

    try:
        docset_info = ADSM.get_docset(docset_id=args.docset_id, exclude_schema=True)
        assert docset_info, "Docset not found"

        return docset_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_aryn_docset_schema(args: GetArynDocSetModel) -> str:
    """Gets the properties of an Aryn DocSet

    Args:
        args: The input arguments defined in the GetArynDocSetModel schema. These include:
        docset_id
    Returns:
        schema: A dictionary of extracted properties of the docset
    """

    try:
        docset_info = ADSM.get_docset(docset_id=args.docset_id, exclude_schema=False)
        assert docset_info, "Docset not found"

        save_file(docset_info["schema"], f"{args.docset_id}_schema", "json")

        return f"File saved in {get_output_dir()} as {args.docset_id}_schema.json"
    except Exception as e:
        return str(e)


@mcp.tool()
def list_aryn_docsets(args: ListArynDocSetsModel) -> list[dict] | dict:
    """Lists all DocSets in the account

    Args:
        args: The input arguments defined in the ListArynDocsetsModel schema. These include:
        page_size
        name_eq
    Returns:
        result: A list of DocSetMetadata dictionaries
    """

    try:
        docsets_info = ADSM.list_docsets(page_size=args.page_size, name_eq=args.name_eq, page_token=args.page_token)
        return docsets_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def delete_aryn_docset(args: DeleteArynDocSetModel) -> dict:
    """Deletes an Aryn DocSet

    Args:
        args: The input arguments defined in the DeleteArynDocsetModel schema. These include:
        docset_id
    Returns:
        result: A DocSetMetadata dictionary of the deleted docset
    """

    try:
        docset_info = ADSM.delete_docset(docset_id=args.docset_id)
        return docset_info
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# Group: Document Managment Functions
# =============================================================================
@mcp.tool()
def add_aryn_document(args: AddArynDocumentModel, options: PartitionModel) -> dict:
    """Adds a document to an Aryn DocSet

    Args:
        args: The input arguments defined in the AddArynDocumentModel schema. These include:
        file
        docset_id
        options: The input arguments defined in the PartitionModel schema. These include:
        threshold
        text_mode
        table_mode
        remove_line_breaks
        include_additional_text
        extract_images
        extract_image_format
        summarize_images
        selected_pages
        strategy
        max_tokens
        tokenizer
        merge_across_pages
        promote_title
        title_candidate_elements
        orientation_correction
        include_pagenum
        include_headers
        include_footers
    Returns:
        result: A DocumentMetadata dictionary of the added document
    """

    try:
        document_info = ADM.add_document(file=args.file, docset_id=args.docset_id, options=options)
        return document_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_aryn_documents(args: ListArynDocumentsModel) -> dict:
    """Lists all documents in an Aryn DocSet

    Args:
        args: The input arguments defined in the ListArynDocumentsModel schema. These include:
        docset_id
        page_size
        page_token
    Returns:
        result: A list of DocumentMetadata dictionaries
    """

    try:
        documents_info = ADM.list_documents(
            docset_id=args.docset_id, page_size=args.page_size, page_token=args.page_token
        )
        return documents_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_aryn_document_elements(args: GetArynDocumentComponentsModel) -> str:
    """Gets a document's elements from an Aryn DocSet document

    Args:
        args: The input arguments defined in the GetArynDocumentComponentsModel schema. These include:
        return_original_elements
        docset_id
        doc_id
    Returns:
        result: a string describing where the contents of the document are saved
    """

    try:
        document_dict = ADM.get_document(
            docset_id=args.docset_id, doc_id=args.doc_id, include_elements=True, include_binary=False
        )

        if args.return_original_elements:
            document_elements = document_dict["original_elements"]
        else:
            document_elements = document_dict["elements"]

        save_file(document_elements, args.doc_id, "json")

        return f"File saved in {get_output_dir()} as {args.doc_id}.json"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_aryn_document_extracted_properties(args: GetArynDocumentExtractedPropertiesModel) -> str:
    """Gets the extracted properties of a document from an Aryn DocSet document

    Args:
        args: The input arguments defined in the GetArynDocumentExtractedPropertiesModel schema. These include:
        docset_id
        doc_id
        output_format
    Returns:
        result: a string describing where the extracted properties are saved
    """
    try:
        document_dict = ADM.get_document(
            docset_id=args.docset_id, doc_id=args.doc_id, include_elements=False, include_binary=False
        )
        document_properties = document_dict["properties"]

        save_file(document_properties, args.doc_id, args.output_format)

        return f"File saved in {get_output_dir()} as {args.doc_id}.{args.output_format}"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_aryn_document_tables(args: GetArynDocumentComponentsModel) -> str:
    """Gets the tables of a document from an Aryn DocSet document

    Args:
        args: The input arguments defined in the GetArynDocumentComponentsModel schema. These include:
        docset_id
        doc_id
    Returns:
        result: a string describing where the tables are saved
    """
    try:
        document_dict = ADM.get_document(
            docset_id=args.docset_id, doc_id=args.doc_id, include_elements=True, include_binary=False
        )
        elements = document_dict["original_elements"]

        tables = tables_to_pandas({"elements": elements})
        tables = [table for _, table in tables if table is not None]

        zip_data = create_zip_from_dataframes(tables)

        save_file(zip_data, args.doc_id, "zip")

        return f"File saved in {get_output_dir()} as {args.doc_id}.zip"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_aryn_document_original_file(args: GetArynDocumentComponentsModel) -> str:
    """Gets the raw data of a document from an Aryn DocSet document

    Args:
        args: The input arguments defined in the GetArynDocumentComponentsModel schema. These include:
        docset_id
        doc_id
    Returns:
        result: a string describing where the original file is saved
    """

    try:
        ADM.get_document_binary(
            docset_id=args.docset_id, doc_id=args.doc_id, file_path=get_output_dir() / f"{args.doc_id}.pdf"
        )

        return f"File saved in {get_output_dir()} as {args.doc_id}.pdf"
    except Exception as e:
        return str(e)


@mcp.tool()
def delete_aryn_document(args: DeleteArynDocumentModel) -> dict:
    """Deletes document from an Aryn DocSet

    Args:
        args: The input arguments defined in the DeleteArynDocumentModel schema. These include:
        docset_id
        doc_id
    Returns:
        result: A DocumentMetadata dictionary of the deleted document or error message
    """

    try:
        document_info = ADM.delete_document(docset_id=args.docset_id, doc_id=args.doc_id)
        return document_info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def extract_aryn_docset_properties(args: ExtractArynDocumentPropertiesModel) -> dict:
    """Extracts properties from all documents in an Aryn DocSet

    Args:
        args: The input arguments defined in the ExtractArynDocumentPropertiesModel schema. These include:
        docset_id
        schema
    Returns:
        result: A job status of the job
    """

    try:
        extraction_status = ADSM.extract_properties(
            docset_id=args.docset_id, properties_to_extract=args.document_schema
        )
        return extraction_status
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def delete_aryn_docset_properties(args: DeleteArynDocSetPropertiesModel) -> dict:
    """Deletes properties from all documents in an Aryn DocSet

    Args:
        args: The input arguments defined in the DeleteArynDocSetPropertiesModel schema. These include:
        docset_id
        schema
    Returns:
        result: A job status of the job
    """
    try:
        deletion_status = ADSM.delete_properties(
            docset_id=args.docset_id, properties_to_delete=args.properties_to_delete
        )
        return deletion_status
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def search_aryn_docset(args: SearchArynDocSetModel) -> dict:
    """Search over a docset and get back documents or elements that match your search criteria

    Args:
        args: The input arguments defined in the SearchArynDocSetModel schema. These include:
        docset_id
        query_or_properties_filter
        query
        query_type
        properties_filter
        k
        return_type
        page_token
    Returns:
        result: A dict of returned attributes
    """
    try:
        search_result = ADSM.search(
            docset_id=args.docset_id,
            query_or_properties_filter=args.query_or_properties_filter,
            query=args.query,
            query_type=args.query_type,
            properties_filter=args.properties_filter,
            page_size=args.page_size,
            return_type=args.return_type,
            page_token=args.page_token,
        )

        return search_result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def query_aryn_docset(args: QueryArynDocSetModel) -> dict:
    """Queries an Aryn DocSet

    Args:
        args: The input arguments defined in the QueryArynDocSetModel schema. These include:
        docset_id
        query
        summarize_result
    """
    try:
        query_result = ADSM.query(docset_id=args.docset_id, query=args.query, summarize_result=args.summarize_result)

        return query_result
    except Exception as e:
        return {"error": str(e)}


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
