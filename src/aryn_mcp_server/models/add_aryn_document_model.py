from pydantic import BaseModel, Field
from os import PathLike


class AddArynDocumentModel(BaseModel):
    """
    Input schema for add_aryn_document()

    Attributes:
        file
        docset_id
    """

    file: str | PathLike = Field(
        ...,
        description="""
            file (str | PathLike, required)
            A file opened in binary mode or a path specified as either a str or PathLike instance indicating the document to add. The path
            can either be a local path or an Amazon S3 url starting with s3://. In the latter case, you must have boto3 installed and AWS
            credentials set up in your environment""",
    )

    docset_id: str = Field(
        ...,
        description="""
            docset_id (str, required)
            The id of the DocSet into which to add the document""",
    )
