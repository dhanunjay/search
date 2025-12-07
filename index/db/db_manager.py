from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.orm import Session, sessionmaker

from db.data_models import Document, DocumentMetadata, IndexStatusType


class DBManager:
    """
    Manages database sessions and provides CRUD operations for Document entities.
    """

    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def get_session(self) -> Session:
        """Helper to get a new session"""
        return self.SessionLocal()

    def create_document(
        self,
        owner_id: int,
        correlation_id: str,
        title: str,
        source_uri: str,
        content_hash: str,
        doc_details: Optional[Dict[str, Any]] = None,
    ) -> Document:
        """
        Creates a new Document record and its associated DocumentMetadata record.
        content_hash is now required and used for linking metadata.
        """
        doc = Document(
            owner_id=owner_id,
            correlation_id=correlation_id,
            title=title,
            source_uri=source_uri,
            content_hash=content_hash,  # content_hash is now NOT NULL
            doc_details=doc_details,
        )
        with self.get_session() as session:
            session.add(doc)
            # Flush or commit is needed before creating metadata to ensure the content_hash exists
            session.flush()

            self._create_pending_metadata(session, content_hash)

            session.commit()
            session.refresh(doc)
            return doc

    def get_document_by_id(self, doc_id: int) -> Optional[Document]:
        """Retrieves a Document by its primary key ID."""
        with self.get_session() as session:
            stmt = select(Document).where(Document.id == doc_id)
            return session.execute(stmt).scalar_one_or_none()

    def get_documents_by_owner(self, owner_id: int) -> List[Document]:
        """Retrieves all Documents belonging to a specific owner."""
        with self.get_session() as session:
            stmt = (
                select(Document)
                .where(Document.owner_id == owner_id)
                .order_by(Document.create_time.desc())
            )
            return list(session.execute(stmt).scalars())

    def update_document_title(
        self, correlation_id: str, new_title: str
    ) -> Optional[Document]:
        """Updates the title of a document based on correlation_id."""
        with self.get_session() as session:
            stmt = (
                update(Document)
                .where(Document.correlation_id == correlation_id)
                .values(title=new_title)
                .returning(Document)
            )
            updated_doc = session.execute(stmt).scalar_one_or_none()
            session.commit()
            return updated_doc

    def delete_document(self, correlation_id: str) -> bool:
        """
        Deletes a document by correlation_id. Associated metadata is deleted via CASCADE.
        """
        with self.get_session() as session:
            stmt = delete(Document).where(
                Document.correlation_id == correlation_id
            )
            result = session.execute(stmt)
            session.commit()
            return result.rowcount > 0

    def _create_pending_metadata(self, session: Session, content_hash: str):
        """
        Internal helper to create an initial PENDING metadata record.
        Now links using doc_content_hash.
        """
        doc_stmt = select(Document).where(Document.content_hash == content_hash)
        doc = session.execute(doc_stmt).scalar_one()

        metadata = DocumentMetadata(
            correlation_id=doc.correlation_id,
            doc_content_hash=content_hash,
            index_status=IndexStatusType.PENDING,
        )
        session.add(metadata)

    def update_index_status(
        self, correlation_id: str, new_status: IndexStatusType
    ) -> Optional[DocumentMetadata]:
        """
        Updates the indexing status of a document based on its content_hash.
        """
        with self.get_session() as session:
            stmt = (
                update(DocumentMetadata)
                .where(DocumentMetadata.correlation_id == correlation_id)
                .values(index_status=new_status)
                .returning(DocumentMetadata)
            )

            updated_metadata = session.execute(stmt).scalar_one_or_none()
            session.commit()
            return updated_metadata

    def create_document_metadata(
        self,
        content_hash: str,
        correlation_id: str,
        new_status: IndexStatusType = IndexStatusType.PENDING,
    ) -> DocumentMetadata:
        """
        Creates a new DocumentMetadata record, linking it to a Document
        via the content_hash and setting the initial status.

        Args:
            content_hash: The content hash of the parent Document.
            correlation_id: The unique job/request ID for this metadata record.
            new_status: The initial IndexStatusType (defaults to PENDING).

        Returns:
            The newly created DocumentMetadata object.
        """
        metadata = DocumentMetadata(
            correlation_id=correlation_id,
            doc_content_hash=content_hash,
            index_status=new_status,
        )
        with self.get_session() as session:
            session.add(metadata)
            session.commit()
            session.refresh(metadata)
            return metadata
