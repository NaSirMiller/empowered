from dotenv import load_dotenv
from getpass import getpass
from functools import lru_cache
import pandas as pd
from sqlmodel import SQLModel
from typing import Any, Dict, List, Type

import os

from buff.models.sql.sql_client import SQLClient


@lru_cache(maxsize=4)
def get_db_client(
    server: str = os.getenv("SQL_SERVER"),
    database: str = os.getenv("SQL_DATABASE"),
    username: str = os.getenv("SQL_USERNAME"),
    driver: str = os.getenv("SQL_DRIVER").replace(" ", "+"),
    password: str = getpass(
        f"Enter SQLServer password for {os.getenv('SQL_USERNAME')}:"
    ),
) -> SQLClient:
    """
    Creates a SQLClient instance for interaction with a SQL database.

    Args:
        server (str, optional): Name of the server to connect to. Defaults to os.getenv("SQL_SERVER").
        database (str, optional): Database to retrieve data from. Defaults to os.getenv("SQL_DATABASE").
        username (str, optional): Name of the user who owns the database. Defaults to os.getenv("SQL_USERNAME").
        driver (str, optional): Drivers needed for SQLServer. Defaults to os.getenv("SQL_DRIVER").replace(" ", "+").
        password (_type_, optional): Password to user's account. Defaults to getpass( f"Enter SQLServer password for {os.getenv('SQL_USERNAME')}:" ).

    Returns:
        SQLClient: _description_
    """
    load_dotenv()
    return SQLClient(
        server=server,
        database=database,
        username=username,
        password=password,
        driver=driver,
    )


from buff.models.sql.sql_client import SQLClient
from buff.utils import get_db_client


@lru_cache(maxsize=128)
def get_matching_from_database(
    model: Type[SQLModel],
    params: Dict[str, Any],
    db_client: SQLClient = get_db_client(),
) -> List[SQLModel]:
    return db_client.select(model=model, filters=params)


def convert_db_results_to_pandas(results: List[SQLModel]) -> pd.DataFrame:
    if len(results) == 0:
        return pd.DataFrame()
    records = [row.model_dump() for row in results]
    return pd.DataFrame(records)
