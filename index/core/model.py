import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, Literal, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)

log = logging.getLogger(__name__)


class IndexDocumentRequest(BaseModel):
    """
    Request model for indexing a document into Elasticsearch.

    Attributes:
        source_url (str): The URL of the document to index.
        content_type (Literal["application/pdf"]): MIME type of the content.
        source_properties (Dict[str, Any]): Any additional properties.
    """

    source_url: str
    content_type: Literal["application/pdf"]
    source_properties: Optional[Dict[str, Any]] = {}
    model_config = {"frozen": True}

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, v: str) -> str:
        """Checks if the source_url is a structurally valid URI."""
        try:
            result = urlparse(v)

            # Require both a scheme (http, file, s3) and a network location (netloc)
            # OR just a scheme and path if it's a file:// URL.
            is_valid_web_url = all([result.scheme, result.netloc])
            is_valid_file_url = result.scheme == "file" and result.path

            if not is_valid_web_url and not is_valid_file_url:
                raise ValueError(
                    "source_url must be a valid URI with a scheme (file://)."
                )

        except ValueError as e:
            # Re-raise the error for Pydantic to catch
            raise ValueError(f"Invalid URL structure: {e}")

        return v


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
    source_url: str
    content_type: str
    source_properties: Dict[str, Any]


class DocumentSource(BaseModel):
    """
    Optimized DocumentSource class where URL parsing is performed once
    and the result is cached for reuse across all methods.
    """

    uri: str = Field(
        ...,
        description="The full Uniform Resource Identifier (URI) of the source.",
    )

    source_properties: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="A dictionary for source-specific details and manipulation data.",
    )

    # Private attribute to store the parsed URL object (ParseResult)
    _parsed_uri: ParseResult = PrivateAttr()

    # --- Pydantic Validation & Initialization (Pydantic V2) ---

    @field_validator("uri")
    @classmethod
    def validate_uri_scheme(cls, v: str):
        """Ensures the URI has a scheme and is supported."""
        parsed = urlparse(v)
        if not parsed.scheme:
            raise ValueError("URI must include a scheme (e.g., 'file://').")

        SUPPORTED_SCHEMES = {"file"}
        if parsed.scheme.lower() not in SUPPORTED_SCHEMES:
            raise ValueError(
                f"Unsupported scheme: {parsed.scheme}. Must be one of {SUPPORTED_SCHEMES}."
            )

        return v

    def model_post_init(self, __context: Any) -> None:
        """Called immediately after successful validation/creation to cache the parsed URI."""
        self._parsed_uri = urlparse(self.uri)

    @property
    def scheme(self) -> str:
        """Returns the cached scheme of the URI (e.g., 's3', 'https')."""
        return self._parsed_uri.scheme.lower()

    def get_local_path(self) -> Optional[str]:
        """
        Resolves the URI to a local file system path, but only for the 'file://' scheme.
        """
        if self.scheme == "file":
            # Accesses the cached path component
            return os.path.abspath(self._parsed_uri.path)

        return None

    def is_validate_local_source(self) -> bool:
        """
        Validates the local file path derived from the URI, returning
        a status (bool, message) instead of raising exceptions.
        """
        file_path = self.get_local_path()

        # 1. Check if Path is resolvable
        if not file_path:
            return False

        # 2. Check if Path Exists
        if not os.path.exists(file_path):
            return False

        # 3. Check if it's a File (and not a Directory)
        if not os.path.isfile(file_path):
            return False

        return True

    def get_source_identifier(self) -> str:
        """
        Returns a canonical identifier for the source (e.g., netloc/path for S3).
        """
        # Accesses the cached netloc and path
        parsed = self._parsed_uri

        if self.scheme == "s3":
            # Format: netloc/path (e.g., bucket/key)
            return f"{parsed.netloc}{parsed.path}"
        if self.scheme in ("http", "https"):
            # Format: netloc/path (without query parameters)
            return f"{parsed.netloc}{parsed.path}"

        # Default to path for file schemes
        return parsed.path

    def update_properties(self, key: str, value: Any):
        """
        Generic method to manipulate source-specific properties in the dictionary.
        """
        if self.source_properties is None:
            self.source_properties = {}
        self.source_properties[key] = value


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
        log.error(
            f"Failed to deserialize/validate Kafka job payload", exc_info=True
        )
        return None
