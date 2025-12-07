import enum
from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import (
    CHAR,
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class which provides automated table name."""

    pass


class IndexStatusType(enum.Enum):
    """Corresponds to the index_status_type ENUM."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Document(Base):
    __tablename__ = "documents"

    # Internal Primary Key
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Relationship and Tracking Keys
    owner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    correlation_id: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True
    )

    # Content Identifiers (Target for Foreign Key)
    content_hash: Mapped[str] = mapped_column(
        CHAR(64), nullable=False, unique=True
    )  # NOTE: Changed to NOT NULL per schema
    title: Mapped[Optional[str]] = mapped_column(String(256))

    # Metadata and Timestamps
    source_uri: Mapped[Optional[str]] = mapped_column(
        CHAR(2048)
    )  # Matched schema length
    doc_details: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now, nullable=False
    )

    metadata_records: Mapped[List["DocumentMetadata"]] = relationship(
        "DocumentMetadata",
        back_populates="document",
        # Explicitly link the relationship using the content_hash column
        primaryjoin="Document.content_hash == DocumentMetadata.doc_content_hash",
        cascade="all, delete-orphan",
    )

    # Custom Index
    __table_args__ = (
        Index("idx_documents_owner_time", owner_id, create_time.desc()),
    )


class DocumentMetadata(Base):
    __tablename__ = "document_metadata"

    # Internal Primary Key
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Linking Key and Status
    correlation_id: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True
    )
    index_status: Mapped[IndexStatusType] = mapped_column(
        Enum(IndexStatusType, native_enum=True, name="index_status_type"),
        nullable=False,
    )

    doc_content_hash: Mapped[str] = mapped_column(
        CHAR(64),
        ForeignKey(
            "documents.content_hash", ondelete="CASCADE"
        ),  # 2. Follow with constraints/arguments
        nullable=False,
    )

    # Relationships
    document: Mapped["Document"] = relationship(
        "Document",
        back_populates="metadata_records",
        primaryjoin="Document.content_hash == DocumentMetadata.doc_content_hash",
        uselist=False,  # One metadata record per content hash (in this specific context/job run)
    )

    # Explicitly define the custom index
    __table_args__ = (Index("idx_document_metadata_status", index_status),)
