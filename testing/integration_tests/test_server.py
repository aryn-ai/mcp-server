import os
import pytest
from pathlib import Path
import json
from aryn_mcp_server.aryn_mcp_server import (
    partition_pdf,
    get_boxes_drawn_on_pdf,
    create_aryn_docset,
    get_aryn_docset_metadata,
    get_aryn_docset_schema,
    list_aryn_docsets,
    delete_aryn_docset,
    add_aryn_document,
    list_aryn_documents,
    get_aryn_document_elements,
    get_aryn_document_extracted_properties,
    get_aryn_document_tables,
    get_aryn_document_original_file,
    delete_aryn_document,
    extract_aryn_docset_properties,
    delete_aryn_docset_properties,
    search_aryn_docset,
    query_aryn_docset,
)
from aryn_mcp_server.models.document_schema import SchemaField
from aryn_mcp_server.models import (
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
    Schema,
    PageRange,
)

from aryn_sdk.client import Client

from dotenv import load_dotenv

load_dotenv()

partition_options = {
    "threshold": 0.5,
    "text_mode": "inline_fallback_to_ocr",
    "table_mode": "standard",
    "text_extraction_options": {"remove_line_breaks": True},
    "table_extraction_options": {"include_additional_text": True},
    "extract_images": False,
}


def extract_file_path_from_message(message: str) -> str:
    if "File saved in" in message:
        parts = message.split(" as ")
        if len(parts) == 2:
            path_part = parts[0].replace("File saved in ", "").strip()
            filename_part = parts[1].strip()
            return str(Path(path_part) / filename_part)

    return message


# Test fixtures
@pytest.fixture
def sample_pdf_path():
    return str(Path("tests") / "data" / "test_1.pdf")


@pytest.fixture
def sample_json_path():
    return str(Path("tests") / "data" / "test.json")


@pytest.fixture
def output_dir():
    return os.getenv("ARYN_MCP_OUTPUT_DIR", str(Path.home() / "Downloads"))


@pytest.fixture(scope="module")
def create_docset():
    client = Client(aryn_api_key=os.getenv("ARYN_API_KEY"))
    schema = Schema(
        fields=[
            SchemaField(
                name="Accident Number",
                field_type="str",
                description="The number of the accident",
                examples=["ERTLJ302242"],
            ),
            SchemaField(
                name="Aircraft Make",
                field_type="str",
                description="The make of the aircraft",
                examples=["Cessna 172"],
            ),
        ]
    )
    docset = client.create_docset(name="integration_test_docset", schema=schema)

    doc_info = client.add_doc(
        file=Path("tests/data/test_1.pdf"),
        docset_id=docset.value.docset_id,
        options=partition_options,
    )

    docset_data = {
        "docset_id": docset.value.docset_id,
        "test_doc_id": doc_info.value.doc_id,
    }

    yield docset_data

    try:
        print(f"\nCleaning up docset: {docset_data['docset_id']}")
        client.delete_docset(docset_id=docset_data["docset_id"])
        print(f"Successfully cleaned up docset: {docset_data['docset_id']}")
    except Exception as e:
        print(f"Warning: Failed to clean up docset {docset_data['docset_id']}: {e}")


def test_partition_pdf(sample_pdf_path):
    args = PartitionModel(
        filename="test_partition",
        file=sample_pdf_path,
        threshold=0.5,
        text_mode="inline_fallback_to_ocr",
        table_mode="standard",
        remove_line_breaks=True,
        include_additional_text=True,
        extract_images=False,
        output_format="json",
    )
    result = partition_pdf(args)
    assert isinstance(result, str)
    assert "File saved" in result

    file_path = extract_file_path_from_message(result)
    assert Path(file_path).exists()


def test_draw_boxes_on_pdf(sample_pdf_path, sample_json_path, create_docset):
    args = DrawBoxesModel(
        docset_id=create_docset["docset_id"],
        doc_id=create_docset["test_doc_id"],
        pages_to_draw_boxes_on=[PageRange(start=1, end=2)],
    )
    result = get_boxes_drawn_on_pdf(args)
    assert isinstance(result, dict)
    assert "saved_image_paths" in result
    assert "saved_image_count" in result
    assert result["saved_image_count"] == 2
    assert len(result["saved_image_paths"]) == 2
    for path in result["saved_image_paths"]:
        assert Path(path).exists()

    args = DrawBoxesModel(
        path_to_partitioned_json=sample_json_path,
        path_to_original_pdf=sample_pdf_path,
        pages_to_draw_boxes_on=[PageRange(start=1, end=2)],
    )
    result = get_boxes_drawn_on_pdf(args)
    assert isinstance(result, dict)
    assert "saved_image_paths" in result
    assert "saved_image_count" in result
    assert result["saved_image_count"] == 2
    assert len(result["saved_image_paths"]) == 2
    for path in result["saved_image_paths"]:
        assert Path(path).exists()


def test_create_aryn_docset():
    schema = Schema(
        fields=[
            SchemaField(
                name="Accident Number",
                field_type="str",
                description="The number of the accident",
                examples=["ERTLJ302242"],
            ),
            SchemaField(
                name="Aircraft Make",
                field_type="str",
                description="The make of the aircraft",
                examples=["Cessna 172"],
            ),
        ]
    )

    args = CreateArynDocSetModel(name="test_docset", schema=schema)
    result = create_aryn_docset(args)
    assert isinstance(result, dict)
    assert "docset_id" in result

    delete_aryn_docset(DeleteArynDocSetModel(docset_id=result["docset_id"]))


def test_get_aryn_docset(create_docset):
    docset_id = create_docset["docset_id"]
    args = GetArynDocSetModel(docset_id=docset_id)
    result = get_aryn_docset_metadata(args)
    assert isinstance(result, dict)
    assert result["docset_id"] == docset_id


def test_list_aryn_docsets():
    args = ListArynDocSetsModel(page_size=10, name_eq="test_docset")
    result = list_aryn_docsets(args)
    assert isinstance(result, list)


def test_delete_aryn_docset():
    result = create_aryn_docset(CreateArynDocSetModel(name="test_docset", schema=None))
    docset_id = result["docset_id"]

    args = DeleteArynDocSetModel(docset_id=docset_id)
    result = delete_aryn_docset(args)
    assert isinstance(result, dict)
    assert result["docset_id"] == docset_id


def test_add_aryn_document(create_docset):
    docset_id = create_docset["docset_id"]

    args = AddArynDocumentModel(file=Path("tests/data/test_2.pdf"), docset_id=docset_id)
    result = add_aryn_document(args, options=PartitionModel(**partition_options))
    assert isinstance(result, dict)
    assert "doc_id" in result


def test_list_aryn_documents(sample_pdf_path, create_docset):
    docset_id = create_docset["docset_id"]

    args = AddArynDocumentModel(file=sample_pdf_path, docset_id=docset_id)
    options = PartitionModel(
        filename="test_partition",
        path=sample_pdf_path,
        threshold=0.5,
        text_mode="inline_fallback_to_ocr",
        table_mode="standard",
    )
    result = add_aryn_document(args, options=options)
    assert isinstance(result, dict)
    assert "doc_id" in result

    args = ListArynDocumentsModel(docset_id=docset_id, page_size=10)
    result = list_aryn_documents(args)
    assert isinstance(result, list)
    assert len(result) > 0


def test_get_document_elements(create_docset):
    docset_id = create_docset["docset_id"]
    doc_id = create_docset["test_doc_id"]
    args = GetArynDocumentComponentsModel(docset_id=docset_id, doc_id=doc_id)
    result = get_aryn_document_elements(args)
    extracted_file_path = extract_file_path_from_message(result)
    assert Path(extracted_file_path).exists()


def test_get_document_extracted_properties(create_docset):
    docset_id = create_docset["docset_id"]
    doc_id = create_docset["test_doc_id"]
    args = GetArynDocumentExtractedPropertiesModel(
        docset_id=docset_id, doc_id=doc_id, output_format="json"
    )
    result = get_aryn_document_extracted_properties(args)
    extracted_file_path = extract_file_path_from_message(result)
    assert Path(extracted_file_path).exists()


def test_get_document_tables(create_docset):
    docset_id = create_docset["docset_id"]
    doc_id = create_docset["test_doc_id"]
    args = GetArynDocumentComponentsModel(docset_id=docset_id, doc_id=doc_id)
    result = get_aryn_document_tables(args)
    extracted_file_path = extract_file_path_from_message(result)
    assert Path(extracted_file_path).exists()


def test_get_document_original_file(create_docset):
    docset_id = create_docset["docset_id"]
    doc_id = create_docset["test_doc_id"]
    args = GetArynDocumentComponentsModel(docset_id=docset_id, doc_id=doc_id)
    result = get_aryn_document_original_file(args)
    extracted_file_path = extract_file_path_from_message(result)
    assert Path(extracted_file_path).exists()


def test_delete_aryn_document(sample_pdf_path, create_docset):
    docset_id = create_docset["docset_id"]

    args = AddArynDocumentModel(
        file=sample_pdf_path,
        docset_id=docset_id,
    )
    result = add_aryn_document(args, options=PartitionModel(**partition_options))
    assert isinstance(result, dict)
    assert "doc_id" in result
    doc_id = result["doc_id"]

    args = DeleteArynDocumentModel(docset_id=docset_id, doc_id=doc_id)
    result = delete_aryn_document(args)
    assert isinstance(result, dict)
    assert "doc_id" in result


def test_extract_delete_aryn_docset_properties(sample_pdf_path, create_docset):
    docset_id = create_docset["docset_id"]

    schema = Schema(
        fields=[
            SchemaField(
                name="Condition of Light",
                field_type="str",
                description="The condition of the time of day during the accident",
                examples=["Day", "Night"],
            )
        ]
    )
    args = ExtractArynDocumentPropertiesModel(docset_id=docset_id, schema=schema)
    extract_aryn_docset_properties(args)

    delete_aryn_docset_properties(
        DeleteArynDocSetPropertiesModel(
            docset_id=docset_id,
            properties_to_delete=["Accident Number", "Aircraft Make"],
        )
    )

    result = get_aryn_docset_schema(GetArynDocSetModel(docset_id=docset_id))
    extracted_file_path = extract_file_path_from_message(result)
    assert Path(extracted_file_path).exists()
    with open(extracted_file_path, "r") as f:
        data = json.load(f)
        assert len(data) == 1


def test_search_aryn_docset(create_docset):
    docset_id = create_docset["docset_id"]

    args = SearchArynDocSetModel(
        docset_id=docset_id,
        query_or_properties_filter="query",
        query="What is the accident number?",
        query_type="lexical",
        page_size=10,
        return_type="doc",
    )
    result = search_aryn_docset(args)

    assert "results" in result


def test_query_aryn_docset(create_docset):
    docset_id = create_docset["docset_id"]

    args = QueryArynDocSetModel(
        docset_id=docset_id,
        query="Where did the accident occur?",
        summarize_result=True,
    )
    result = query_aryn_docset(args)
    assert isinstance(result, dict)
    assert "summary" in result
    assert "doc_id" in result
