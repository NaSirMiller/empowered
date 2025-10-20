from functools import lru_cache
import pandas as pd
from sqlmodel import SQLModel
from typing import Any, Dict, List, Type

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
