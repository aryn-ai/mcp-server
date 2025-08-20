import os
import unittest
from pathlib import Path

from aryn_sdk.client import Client
from aryn_mcp_server.models import PartitionModel
from aryn_mcp_server.aryn_document_manager import ArynDocumentManager

from dotenv import load_dotenv

load_dotenv()


class TestArynDocumentManager(unittest.TestCase):
    def setUp(self):
        self.client = Client(aryn_api_key=os.getenv("ARYN_API_KEY"))
        self.ADM = ArynDocumentManager(aryn_api_key=os.getenv("ARYN_API_KEY"))

        docset = self.client.create_docset(name="test_document_unittest")
        self.test_docset_id = docset.value.docset_id
        self.test_file_path = Path("tests/data/test_1.pdf")

        self.partition_options = {
            "threshold": 0.5,
            "text_mode": "inline_fallback_to_ocr",
            "table_mode": "standard",
            "text_extraction_options": {"remove_line_breaks": True},
            "table_extraction_options": {"include_additional_text": True},
            "extract_images": False,
        }

        doc_info = self.client.add_doc(
            file=self.test_file_path, docset_id=self.test_docset_id, options=self.partition_options
        )
        self.test_doc_id = doc_info.value.doc_id

    def tearDown(self):
        try:
            self.client.delete_docset(docset_id=self.test_docset_id)
        except Exception as e:
            print(f"Error deleting docset {self.test_docset_id}: {e}")
            pass

    def test_add_get_delete_document(self):
        doc_info = self.ADM.add_document(
            file=Path("tests/data/test_2.pdf"),
            docset_id=self.test_docset_id,
            options=PartitionModel(**self.partition_options),
        )
        self.assertIsNotNone(doc_info["doc_id"])
        self.assertIsNotNone(doc_info["name"])
        self.assertIsNotNone(doc_info["size"])
        self.assertIsNotNone(doc_info["content_type"])

        doc_id = doc_info["doc_id"]
        get_result = self.ADM.get_document(
            docset_id=self.test_docset_id, doc_id=doc_id, include_elements=True, include_binary=True
        )
        self.assertIsNotNone(get_result)
        self.assertEqual(get_result["doc_id"], doc_id)
        self.assertIsNotNone(get_result["elements"])
        self.assertIsNotNone(get_result["binary_data"])

        delete_result = self.ADM.delete_document(docset_id=self.test_docset_id, doc_id=doc_id)
        self.assertEqual(delete_result["doc_id"], doc_id)

        get_result = self.ADM.get_document(
            docset_id=self.test_docset_id, doc_id=doc_id, include_elements=True, include_binary=True
        )
        self.assertIsNone(get_result)

    def test_list_documents(self):
        docs_info = self.ADM.list_documents(docset_id=self.test_docset_id, page_size=10, page_token=None)

        for doc_info in docs_info:
            self.assertIsNotNone(doc_info["doc_id"])
            self.assertIsNotNone(doc_info["name"])
            self.assertIsNotNone(doc_info["size"])
            self.assertIsNotNone(doc_info["content_type"])

    def test_get_document_not_found(self):
        get_result = self.ADM.get_document(
            docset_id=self.test_docset_id, doc_id="non_existent_doc_id", include_elements=True, include_binary=True
        )
        self.assertIsNone(get_result)

    def test_add_document_invalid_docset(self):
        with self.assertRaises(Exception):
            self.ADM.add_document(file=self.test_file_path, docset_id="non_existent_docset_id")

    def test_delete_document_not_found(self):
        with self.assertRaises(Exception):
            self.ADM.delete_document(docset_id=self.test_docset_id, doc_id="non_existent_doc_id")

    def test_get_document_properties(self):
        get_result = self.ADM.get_document(
            docset_id=self.test_docset_id, doc_id=self.test_doc_id, include_elements=True, include_binary=True
        )
        self.assertIsNotNone(get_result)
        self.assertIsNotNone(get_result["properties"])


if __name__ == "__main__":
    unittest.main()
