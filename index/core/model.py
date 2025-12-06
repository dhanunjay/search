import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, model_validator

log = logging.getLogger(__name__)


class IndexDocumentRequest(BaseModel):
    """
    Request model for indexing a document into Elasticsearch.

    Attributes:
        source_type (Literal["local_file"]): The type of source. Currently only 'local_file' is supported.
        content_type (Literal["application/pdf"]): MIME type of the content. Currently only 'application/pdf' is supported.
        source_properties (Dict[str, Any]): Properties specific to the source type.
            - For local_file: {"file_path": "/path/to/file.pdf"}
    """

    source_type: Literal["local_file"]

    content_type: Literal["application/pdf"]

    source_properties: Dict[str, Any]

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def validate_source_properties(self) -> "IndexDocumentRequest":
        if self.source_type == "local_file":
            if "file_path" not in self.source_properties:
                raise ValueError(
                    "source_properties must include 'file_path' for local_file"
                )

            if not self.source_properties.get("file_path"):
                raise ValueError("file_path cannot be empty for local_file")
        return self


class IndexDocumentResponse(BaseModel):
    """
    Response model for indexing operations.

    Attributes:
        job_id (str): Unique identifier for the indexing job.
        indexing_status (str): Status of the indexing operation (e.g., 'completed').
        metadata (dict[str, Any]): Additional metadata such as file path and number of chunks indexed.
    """

    job_id: str
    indexing_status: str
    metadata: dict[str, Any]

    class Config:
        frozen = True


@dataclass
class IndexDocumentJob:
    job_id: str
    source_type: str
    content_type: str
    source_properties: Dict[str, Any]


def default_serializer(data: Optional[Any]) -> Optional[bytes]:
    """Default serializer: string -> UTF-8 bytes."""
    return str(data).encode("utf-8") if data is not None else None


def default_deserializer(data: Optional[bytes]) -> Optional[str]:
    """Default deserializer: bytes -> UTF-8 string."""
    return data.decode("utf-8") if data else None


def custom_serializer(data: Union[IndexDocumentJob, Any]) -> Optional[bytes]:
    """
    Serializes IndexDocumentJob (now a dataclass) to JSON bytes.
    """
    if data is None:
        return None

    if isinstance(data, IndexDocumentJob):
        # Use asdict() for clean conversion of the dataclass to a standard dictionary
        data_dict = asdict(data)
    else:
        # Fallback for simple data types (assuming IndexDocumentRequest is removed)
        try:
            # Using the standard json serializer if data is already a simple dict/list
            data_dict = data
        except TypeError:
            return None  # Handle cases where data is not serializable

    # Final JSON serialization
    return json.dumps(data_dict).encode("utf-8")


def custom_deserializer(data: Optional[bytes]) -> Optional[IndexDocumentJob]:
    """
    Deserializes JSON bytes into a single IndexDocumentJob dataclass object.
    """
    if data is None:
        return None

    try:
        # 1. Decode bytes and parse JSON
        data_str = data.decode("utf-8")
        job_dict = json.loads(data_str)

        # 2. Construct the single dataclass object
        # The constructor handles assigning the keys from the dictionary
        job_object = IndexDocumentJob(**job_dict)

        return job_object

    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
        log.error(f"Failed to deserialize/validate Kafka job payload", exc_info=True)
        return None
