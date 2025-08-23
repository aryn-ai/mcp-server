from os import PathLike
from .models import PartitionModel

from aryn_sdk.client import Client


class ArynDocumentManager:
    def __init__(self, aryn_api_key: str | None = None, aryn_url: str | None = None):
        if aryn_api_key and aryn_url:
            self.client = Client(aryn_api_key=aryn_api_key, aryn_url=aryn_url)
        else:
            self.client = Client()

    def _create_doc_info(self, doc, listing: bool = False) -> dict:
        doc_params = doc

        if not listing:
            doc_params = doc.value

        docset_dict = {
            "account_id": doc_params.account_id,
            "doc_id": doc_params.doc_id,
            "name": doc_params.name,
            "size": doc_params.size,
            "content_type": doc_params.content_type,
            "properties": doc_params.properties,
        }

        return docset_dict

    def _extract_properties(self, properties):
        if not isinstance(properties, dict):
            return properties

        if "entity" in properties:
            return self._extract_properties(properties["entity"])

        return properties

    def _create_partition_options(self, options: PartitionModel):
        partition_options = {
            "threshold": options.threshold,
            "text_mode": options.text_mode,
            "table_mode": options.table_mode,
            "text_extraction_options": {"remove_line_breaks": options.remove_line_breaks},
            "table_extraction_options": {"include_additional_text": options.include_additional_text},
            "extract_images": options.extract_images,
            "extract_image_format": options.extract_image_format,
            "summarize_images": options.summarize_images,
            "selected_pages": options.selected_pages,
            "chunking_options": {
                "strategy": options.strategy,
                "max_tokens": options.max_tokens,
                "tokenizer": options.tokenizer,
                "merge_across_pages": options.merge_across_pages,
            },
            "output_label_options": {
                "promote_title": options.promote_title,
                "title_candidate_elements": options.title_candidate_elements,
                "orientation_correction": options.orientation_correction,
            },
            "markdown_options": {
                "include_pagenum": options.include_pagenum,
                "include_headers": options.include_headers,
                "include_footers": options.include_footers,
            },
        }
        return partition_options

    def add_document(self, file: str | PathLike, docset_id: str, options: PartitionModel):
        try:
            partition_options = self._create_partition_options(options)
            doc = self.client.add_doc(file=file, docset_id=docset_id, options=partition_options)
            doc_info = self._create_doc_info(doc)
            return doc_info
        except Exception as e:
            raise Exception(f"Failed to add document to docset {docset_id}: {str(e)}") from e

    def list_documents(self, docset_id: str, page_size: int, page_token: str | None):
        try:
            docs = self.client.list_docs(docset_id=docset_id, page_size=page_size, page_token=page_token)
            docs_info = [self._create_doc_info(doc, listing=True) for doc in docs]
            return docs_info
        except Exception as e:
            raise Exception(f"Failed to list documents in docset {docset_id}: {str(e)}") from e

    def get_document(self, docset_id: str, doc_id: str, include_elements: bool, include_binary: bool):
        try:
            doc = self.client.get_doc(
                docset_id=docset_id,
                doc_id=doc_id,
                include_elements=include_elements,
                include_binary=include_binary,
            )
            doc = doc.value

            element_dict = {}
            if include_elements:
                for element in doc.elements:
                    element_dict[element.id] = {
                        "id": element.id,
                        "type": element.type,
                        "text_representation": element.text_representation,
                        "properties": element.properties,
                        "bbox": element.bbox,
                    }

            properties = self._extract_properties(doc.properties)

            doc_info = {
                "doc_id": doc.id,
                "elements": element_dict,
                "properties": properties,
                "binary_data": doc.binary_data,
                "original_elements": doc.properties["_original_elements"],
            }
            return doc_info
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return None
            raise Exception(f"Failed to get document {doc_id} in docset {docset_id}: {str(e)}") from e

    def delete_document(self, docset_id: str, doc_id: str):
        try:
            doc = self.client.delete_doc(docset_id=docset_id, doc_id=doc_id)
            doc_info = self._create_doc_info(doc)
            return doc_info
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                raise Exception(f"Document {doc_id} not found in docset {docset_id}") from e
            raise Exception(f"Failed to delete document {doc_id} in docset {docset_id}: {str(e)}") from e

    def get_document_binary(self, docset_id: str, doc_id: str, file_path: str | PathLike):
        try:
            self.client.get_doc_binary(docset_id=docset_id, doc_id=doc_id, file=file_path)
            return file_path
        except Exception as e:
            raise Exception(f"Failed to get document binary for {doc_id} in docset {docset_id}: {str(e)}") from e
