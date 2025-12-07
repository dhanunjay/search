import logging
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

log = logging.getLogger(__name__)

try:
    from data_models import Base, Document, DocumentMetadata, IndexStatusType
except ImportError:
    # If models are defined in the same script, uncomment the following:
    # from __main__ import Base, IndexStatusType, Document, DocumentMetadata
    log.error(
        "Error: Could not import models from 'data_models.py'. "
        "Please ensure your models are saved in that file, or adjust the import."
    )
    sys.exit(1)


# IMPORTANT: Replace this with your actual PostgreSQL connection string.
# Format: postgresql+psycopg2://user:password@host:port/dbname
DATABASE_URL = (
    "postgresql+psycopg2://test:testpwd123@localhost:5432/hybridsearch"
)


def create_custom_enum(engine):
    """
    Creates the PostgreSQL native ENUM type (index_status_type) if it doesn't exist.
    """
    print("Checking/Creating custom ENUM type 'index_status_type'...")
    enum_name = "index_status_type"

    # Generate the SQL to create the ENUM based on the Python enum members
    enum_values = [f"'{member.value}'" for member in IndexStatusType]
    create_type_sql = text(
        f"""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_name}') THEN
                CREATE TYPE {enum_name} AS ENUM ({', '.join(enum_values)});
                RAISE NOTICE 'Created ENUM type: {enum_name}';
            END IF;
        END $$;
    """
    )

    with engine.connect() as connection:
        try:
            # SQLAlchemy runs the transactional block
            connection.execute(create_type_sql)
            connection.commit()
        except ProgrammingError as e:
            # Handle cases where the type might be partially created or permissions are off
            print(
                f"Warning: Could not create ENUM type. Check permissions or existing type. Error: {e}"
            )


def setup_database():
    """
    Connects to the database and sets up the schema.
    """
    log.info(f"Connecting to database at: {DATABASE_URL.split('@')[-1]}")
    try:
        # Create the engine
        engine = create_engine(DATABASE_URL)

        # 1. Create the custom ENUM type first, as it's needed by the tables
        create_custom_enum(engine)

        # 2. Create all defined tables (documents and document_metadata)
        log.info(
            "Creating tables (documents, document_metadata) if they do not exist..."
        )
        Base.metadata.create_all(engine)
        log.info("Database setup complete! Tables and indexes are ready.")

    except Exception as e:
        log.error(
            f"\nFATAL ERROR: Could not connect or create database structure. Please check your DATABASE_URL, ensure the database exists, and the PostgreSQL server is running.",
            exc_info=True,
        )
        sys.exit(1)


# --- Script Execution ---

if __name__ == "__main__":
    setup_database()
