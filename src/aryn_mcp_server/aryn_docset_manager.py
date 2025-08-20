from aryn_sdk.client import Client
from aryn_sdk.client.client import SearchRequest

from aryn_sdk.types.query import Query
from aryn_sdk.types.schema import Schema, SchemaField

from typing import Literal
from .models import PropertiesFilterModel

from collections import defaultdict


class ArynDocSetManager:
    def __init__(self):
        self.client = Client()

    def _parse_through_schema(self, schema: list[SchemaField]):
        properties_list = [vars(property) for property in schema]
        return properties_list

    def _generate_docset_info(self, docset, listing: bool = False, exclude_schema: bool = False) -> dict:
        docset_params = docset
        if not listing:
            docset_params = docset.value

        docset_info = {
            "docset_id": docset_params.docset_id,
            "name": docset_params.name,
            "readonly": docset_params.readonly,
            "properties": docset_params.properties,
            "schema": (self._parse_through_schema(docset_params.schema_.properties) if docset_params.schema_ else None),
            "size": docset_params.size,
        }

        if exclude_schema:
            docset_info.pop("schema")

        return docset_info

    def _generate_properties_filter_string(self, properties_filter: list[PropertiesFilterModel]):
        properties_filter_string = ""
        for filter in properties_filter:
            if filter.property_type == "str" or filter.property_type == "bool":
                properties_filter_string += (
                    f"""(properties.entity."{filter.property}"{filter.operator}"{filter.value}") AND """
                )
            elif filter.property_type == "int":
                properties_filter_string += (
                    f"""(properties.entity."{filter.property}"{filter.operator}{filter.value}) AND """
                )

        properties_filter_string = properties_filter_string[:-5]

        return properties_filter_string

    def create_docset(self, name: str, schema: Schema | None) -> dict:
        try:
            docset = self.client.create_docset(name=name, schema=schema)
            created_docset_info = self._generate_docset_info(docset)

            return created_docset_info

        except Exception as e:
            raise Exception(f"Failed to create docset {name}: {str(e)}") from e

    def get_docset(self, docset_id: str, exclude_schema: bool = False) -> dict | None:
        try:
            docset = self.client.get_docset(docset_id=docset_id)
            docset_info = self._generate_docset_info(docset, exclude_schema=exclude_schema)

            return docset_info

        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return None
            raise Exception(f"Failed to get docset {docset_id}: {str(e)}") from e

    def delete_docset(self, docset_id: str) -> dict:
        try:
            docset = self.client.delete_docset(docset_id=docset_id)
            deleted_docset_info = self._generate_docset_info(docset)

            return deleted_docset_info

        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                raise Exception(f"Docset {docset_id} not found: {str(e)}") from e
            raise Exception(f"Failed to delete docset {docset_id}: {str(e)}") from e

    def list_docsets(self, page_size: int, name_eq: str | None = None, page_token: str | None = None) -> list[dict]:
        try:
            if name_eq:
                docsets = self.client.list_docsets(
                    page_size=page_size, name_eq=name_eq, page_token=page_token
                ).get_all()
            else:
                docsets = self.client.list_docsets(page_size=page_size).get_all()

            docsets_info = [self._generate_docset_info(d, True, True) for d in docsets]
            return docsets_info

        except Exception as e:
            raise Exception(f"Failed to list docsets: {str(e)}") from e

    def extract_properties(self, docset_id: str, properties_to_extract: Schema) -> dict:
        try:
            # NOTE: Extracting a property from an empty docset will do nothing, pending fix from Aryn
            docset_info = self.get_docset(docset_id)
            if not docset_info or docset_info["size"] is None or docset_info["size"] == 0:
                raise Exception(f"Docset {docset_id} is empty, cannot extract properties")

            result = self.client.extract_properties(docset_id=docset_id, schema=properties_to_extract)
            result = result.value

            if result.exit_status == 0:
                return {
                    "docset_id": docset_id,
                    "extracted_properties": [p.name for p in properties_to_extract.fields],
                }
            else:
                raise Exception(f"Exit status: {result.exit_status}")

        except Exception as e:
            raise Exception(f"Failed to extract properties for docset {docset_id}: {str(e)}") from e

    def delete_properties(self, docset_id: str, properties_to_delete: list[str]) -> dict:

        try:
            result = self.client.delete_properties(docset_id=docset_id, property_names=properties_to_delete)

            result = result.value

            if result.exit_status == 0:
                return {
                    "docset_id": docset_id,
                    "deleted_properties": properties_to_delete,
                }
            else:
                raise Exception(f"Exit status: {result.exit_status}")
        except Exception as e:
            raise Exception(f"Failed to delete properties for docset {docset_id}: {str(e)}") from e

    def search(
        self,
        docset_id: str,
        query_or_properties_filter: Literal["query", "properties_filter"],
        query: str | None,
        query_type: Literal["keyword", "vector", "lexical", "hybrid"] | None,
        properties_filter: list[PropertiesFilterModel] | None,
        page_size: int,
        return_type: Literal["doc", "element"],
        page_token: str | None,
    ) -> dict:

        properties_filter_string = None
        if properties_filter is not None:
            properties_filter_string = self._generate_properties_filter_string(properties_filter=properties_filter)

        try:
            if query_or_properties_filter == "query":
                search_result = self.client.search(
                    docset_id=docset_id,
                    query=SearchRequest(
                        query=query,
                        query_type=query_type,
                        include_fields=["doc_id"],
                        return_type=return_type,
                    ),
                    page_size=page_size,
                )
            elif query_or_properties_filter == "properties_filter":
                search_result = self.client.search(
                    docset_id=docset_id,
                    query=SearchRequest(
                        properties_filter=properties_filter_string,
                        include_fields=["doc_id"],
                        return_type=return_type,
                    ),
                    page_size=page_size,
                )

            search_result = search_result.value

            return {
                "results": search_result.results,
                "next_page_token": search_result.next_page_token,
            }
        except Exception as e:
            raise Exception(f"Failed to search docset {docset_id}: {str(e)}") from e

    def query(self, docset_id: str, query: str, summarize_result: bool):

        try:
            query_result = self.client.query(
                query=Query(
                    query=query,
                    docset_id=docset_id,
                    summarize_result=summarize_result,
                    stream=True,
                )
            )

            query_result_data = defaultdict(str)
            for event in query_result:
                if event.event_type == "trace_doc":
                    query_result_data["doc_id"] = event.data.doc["doc_id"]
                elif event.event_type == "result_summary":
                    query_result_data["summary"] += event.data
                elif event.event_type == "complete" and event.data[-9:] == "0 results":
                    query_result_data["summary"] = "No results found"
                    query_result_data["doc_id"] = "No results found"

            return dict(query_result_data)
        except Exception as e:
            raise Exception(f"Failed to query docset {docset_id}: {str(e)}") from e
