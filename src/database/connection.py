"""
Database connection utilities for investment analytics pipeline
"""
import os
import logging
from typing import Optional
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Manages PostgreSQL database connections with connection pooling"""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or self._get_connection_string()
        self.engine = self._create_engine()
    
    def _get_connection_string(self) -> str:
        """Build connection string from environment variables"""
        postgres_url = os.getenv('POSTGRES_URL')
        if postgres_url:
            return postgres_url
        
        # Fallback to individual components
        # Use container-friendly defaults
        host = os.getenv('DB_HOST', os.getenv('POSTGRES_HOST', 'postgres'))  # Changed from localhost to postgres for Docker
        port = os.getenv('DB_PORT', os.getenv('POSTGRES_PORT', '5432'))
        db = os.getenv('DB_NAME', os.getenv('POSTGRES_DB', 'investment_analytics'))  # Changed from investment_db to investment_analytics
        user = os.getenv('DB_USER', os.getenv('POSTGRES_USER', 'postgres'))
        password = os.getenv('DB_PASSWORD', os.getenv('POSTGRES_PASSWORD', 'password'))
        
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=os.getenv('DEBUG', 'False').lower() == 'true'
        )
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise
    
    def execute_statement(self, statement: str, params: dict = None) -> None:
        """Execute an INSERT/UPDATE/DELETE statement"""
        try:
            with self.get_connection() as conn:
                with conn.begin():
                    conn.execute(text(statement), params or {})
        except Exception as e:
            logger.error(f"Statement execution error: {e}")
            raise
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = 'append', index: bool = False) -> None:
        """Insert DataFrame into database table"""
        try:
            with self.get_connection() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=index)
                logger.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"DataFrame insertion error: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

# Global database instance
_db_instance = None

def get_db_connection():
    """Get global database connection instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance

def get_raw_connection():
    """Get raw psycopg2 connection for legacy code"""
    db = get_db_connection()
    return psycopg2.connect(db.connection_string)
