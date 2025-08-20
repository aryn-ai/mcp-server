import os
import unittest
from pathlib import Path

from aryn_sdk.client import Client
from aryn_mcp_server.models import Schema
from aryn_mcp_server.models.document_schema import SchemaField
from aryn_mcp_server.models import PropertiesFilterModel
from aryn_mcp_server.aryn_docset_manager import ArynDocSetManager

from dotenv import load_dotenv

load_dotenv()


class TestArynDocSetManager(unittest.TestCase):
    def setUp(self):
        self.client = Client(aryn_api_key=os.getenv("ARYN_API_KEY"))
        self.ADSM = ArynDocSetManager(aryn_api_key=os.getenv("ARYN_API_KEY"))
        self.test_schema = Schema(
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
        self.test_docset_name = "test_docset_unittest"

        creation_result = self.client.create_docset(name=self.test_docset_name, schema=self.test_schema)
        self.test_docset_id = creation_result.value.docset_id
        self.client.add_doc(docset_id=self.test_docset_id, file=Path("tests/data/test_1.pdf"))
        self.client.add_doc(docset_id=self.test_docset_id, file=Path("tests/data/test_2.pdf"))

    def tearDown(self):
        self.ADSM.delete_docset(self.test_docset_id)

    def test_create_delete_docset(self):
        docset_info = self.ADSM.create_docset("test_create_delete_docset", None)
        docset_id = docset_info["docset_id"]

        with self.assertRaises(Exception):
            self.ADSM.extract_properties(docset_id, Schema(fields=[]))

        self.assertEqual(docset_info["name"], "test_create_delete_docset")
        self.assertIsNotNone(docset_id)
        self.assertIsNone(docset_info["size"])

        self.ADSM.delete_docset(docset_id)
        self.assertIsNone(self.ADSM.get_docset(docset_id))

        with self.assertRaises(Exception):
            self.ADSM.create_docset("", [])

    def test_delete_docset_not_found(self):
        with self.assertRaises(Exception):
            self.ADSM.delete_docset("what_up_DJ")

    def test_list_docsets(self):
        docsets_info = self.ADSM.list_docsets(page_size=10)

        for docset_info in docsets_info:
            self.assertIsNotNone(docset_info["docset_id"])
            self.assertIsNotNone(docset_info["name"])

    def test_extract_delete_properties(self):
        new_property = SchemaField(
            name="Condition of Light",
            field_type="str",
            description="The condition of the time of day during the accident",
            examples=["Day", "Night"],
        )
        new_schema = Schema(fields=[new_property])

        self.ADSM.extract_properties(self.test_docset_id, new_schema)

        docset_info = self.ADSM.get_docset(self.test_docset_id)
        self.assertEqual(len(docset_info["schema"]), 3)
        self.assertEqual(docset_info["schema"][0]["name"], new_property.name)
        self.assertEqual(docset_info["schema"][0]["property_type"], new_property.field_type)
        self.assertEqual(docset_info["schema"][0]["description"], new_property.description)
        self.assertEqual(docset_info["schema"][0]["examples"], new_property.examples)

        self.ADSM.delete_properties(self.test_docset_id, ["Accident Number", "Aircraft Make"])
        docset_info = self.ADSM.get_docset(self.test_docset_id)
        self.assertEqual(len(docset_info["schema"]), 1)
        self.assertEqual(docset_info["schema"][0]["name"], new_property.name)
        self.assertEqual(docset_info["schema"][0]["property_type"], new_property.field_type)
        self.assertEqual(docset_info["schema"][0]["description"], new_property.description)
        self.assertEqual(docset_info["schema"][0]["examples"], new_property.examples)

    def test_search_docset(self):
        properties_filter = [
            PropertiesFilterModel(property="Accident Number", property_type="str", value="DCA25MA108", operator="=")
        ]
        search_result = self.ADSM.search(
            docset_id=self.test_docset_id,
            query_or_properties_filter="properties_filter",
            query=None,
            query_type=None,
            properties_filter=properties_filter,
            return_type="doc",
            page_size=10,
            page_token=None,
        )
        doc_id = search_result["results"][0]["doc_id"]
        doc_info = self.client.get_doc(docset_id=self.test_docset_id, doc_id=doc_id)
        self.assertEqual(doc_info.value.properties["entity"]["Accident Number"], "DCA25MA108")

        search_result = self.ADSM.search(
            docset_id=self.test_docset_id,
            query_or_properties_filter="query",
            query="Which document is the one that has the accident number DCA25MA108?",
            query_type="lexical",
            properties_filter=None,
            return_type="doc",
            page_size=10,
            page_token=None,
        )
        doc_id = search_result["results"][0]["doc_id"]
        doc_info = self.client.get_doc(docset_id=self.test_docset_id, doc_id=doc_id)
        self.assertEqual(doc_info.value.properties["entity"]["Accident Number"], "DCA25MA108")

        with self.assertRaises(Exception):
            search_result = self.ADSM.search(
                docset_id=self.test_docset_id,
                query_or_properties_filter="query",
                query=None,
                query_type=None,
                properties_filter=[],
                return_type="doc",
                page_size=10,
                page_token=None,
            )

        with self.assertRaises(Exception):
            search_result = self.ADSM.search(
                docset_id=self.test_docset_id,
                query_or_properties_filter="properties_filter",
                query=None,
                query_type=None,
                properties_filter=None,
                return_type="doc",
                page_size=10,
                page_token=None,
            )

    def test_query_docset(self):
        query_result = self.ADSM.query(
            docset_id=self.test_docset_id,
            query="What is the accident number involving an airplane?",
            summarize_result=True,
        )

        self.assertIn("ERA23LA117", query_result["summary"])
        self.assertIn("doc_id", query_result)

    # def test_query_docset_weapons(self):
    #     query_result = self.ADSM.query(
    #         docset_id="aryn:ds-dzl7kvssl7ob2pcoahlmtsg",
    #         query="most expensive project highest cost largest budget allocation",
    #         summarize_result=True
    #     )


if __name__ == "__main__":
    unittest.main()
