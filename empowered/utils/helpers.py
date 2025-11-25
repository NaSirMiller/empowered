from dotenv import load_dotenv
from functools import lru_cache
import pandas as pd
from sqlmodel import SQLModel
from typing import Any, Dict, List, Type

import os

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.db import db_client


@lru_cache(maxsize=1)
def get_census_api_key(key_name: str = "CENSUS_API_KEY") -> str:
    load_dotenv()
    return os.getenv(key_name)


def get_sql_client() -> SQLClient:
    """
    Loads SQL server client instance.

    Returns:
        SQLClient: _description_
    """
    return db_client


from empowered.models.sql.sql_client import SQLClient
from empowered.utils.helpers import get_sql_client


@lru_cache(maxsize=128)
def get_matching_from_database(
    model: Type[SQLModel],
    params: Dict[str, Any],
    db_client: SQLClient = get_sql_client(),
) -> List[SQLModel]:
    return db_client.select(model=model, filters=params)


def convert_db_results_to_pandas(results: List[SQLModel]) -> pd.DataFrame:
    if len(results) == 0:
        return pd.DataFrame()
    records = [row.model_dump() for row in results]
    return pd.DataFrame(records)


# async def bound_fetch(
#     url: str,
#     fetch_method: Callable,
#     semaphore: asyncio.Semaphore,
#     session: ClientSession,
#     **kwargs,
# ):
#     async with semaphore:
#         return await fetch_method(url, session, **kwargs)
