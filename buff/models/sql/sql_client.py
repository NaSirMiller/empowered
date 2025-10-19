import pyodbc
from typing import Any, Dict, List

from logger_setup import get_logger

logger = get_logger(name=__name__)


class SQLClient:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

        conn_str = f"""
            DRIVER={{{driver}}};
            SERVER={server};
            DATABASE={database};
            UID={username};
            PWD={password};
            TrustServerCertificate=yes;
            Encrypt=no;
        """
        logger.info("Connecting to SQL server...")
        try:
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error(f"Error occurred while connecting to server: {server}")
            raise Exception(e)
        logger.info("Server successfully established.")

    def execute(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes the provided query and provides the results.

        Args:
            query (str): SQL query in TSQL syntax

        Raises:
            Exception: Error that occurred while executing query

        Returns:
            List[Dict[str, Any]]:
        """
        try:
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            rows: list[pyodbc.Row] = self.cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            return data
        except Exception as e:
            logger.error(f"Error executing query={query}: {e}")
            raise Exception(e)

    def close(self) -> None:
        self.conn.close()
        logger.info("Server connection closed.")
