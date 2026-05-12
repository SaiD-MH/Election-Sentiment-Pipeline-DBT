import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL

load_dotenv()


class DatabaseConnection:
    """
    Manages Snowflake database connections for ETL operations.
    Implements connection pooling and proper resource management.
    """

    def __init__(
        self,
        account: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):

        self.account = account or os.getenv("DB_ACCOUNT")
        self.user = user or os.getenv("DB_USER")
        self.password = password or os.getenv("DB_PWD")

        # Snowflake requires all identifiers to be UPPERCASE
        self.database = (database or os.getenv("DB_NAME", "")).upper()
        self.schema = (schema or os.getenv("DB_SCHEMA", "")).upper()
        self.warehouse = (warehouse or os.getenv("DB_WAREHOUSE", "")).upper()
        self.role = (role or os.getenv("DB_ROLE", "")).upper()

        self._validate_db_config()
        self._engine: Optional[Engine] = None

    def _validate_db_config(self):
        """Validate required configuration."""
        required = {
            "account": self.account,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "user": self.user,
            "password": self.password,
        }

        missing = [key for key, value in required.items() if not value]

        if missing:
            raise ValueError(
                f"Missing required database configuration: {', '.join(missing)}. "
                "Check your .env file."
            )

    from snowflake.sqlalchemy import URL

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            try:
                self._engine = create_engine(
                    URL(
                        user=self.user,
                        password=self.password,
                        account=self.account,
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                        role=self.role,
                    ),
                    pool_pre_ping=True,
                    pool_size=5,
                    max_overflow=10,
                    echo=False,
                )

                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

            except Exception as e:
                raise ConnectionError(f"Database connection failed: {e}") from e

        return self._engine

    def _snowflake_connection(self):
        """
        Native Snowflake connector (used for fast dataframe loading).
        """

        kwargs = dict(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        if self.role:
            kwargs["role"] = self.role

        return snowflake.connector.connect(**kwargs)

    def load_dataframe_into_db(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> int:
        """
        Load DataFrame into Snowflake using optimized bulk loading.
        Column names are uppercased to match Snowflake's identifier requirements.

        Returns number of rows inserted.
        """

        if df.empty:
            raise ValueError("The DataFrame is empty.")

        # Copy to avoid mutating the caller's DataFrame
        df = df.copy()
        df.columns = df.columns.str.upper()

        try:
            with self._snowflake_connection() as conn:
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df,
                    table_name.upper(),
                    schema=self.schema,
                )

            if not success:
                raise RuntimeError("Snowflake write_pandas failed.")

            return nrows

        except Exception as e:
            raise RuntimeError(f"Failed to insert dataframe: {e}") from e

    def read_dataframe_from_db(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as pandas DataFrame.

        # TODO: parameterize queries to prevent SQL injection before production use.
        """

        try:
            df = pd.read_sql(query, con=self.engine)
            return df

        except Exception as e:
            raise Exception(f"Failed to read dataframe: {e}") from e

    def close(self):
        """Dispose SQLAlchemy engine."""

        if self._engine:
            self._engine.dispose()
            self._engine = None

    def __enter__(self):
        """Allow usage with 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection cleanup."""
        self.close()