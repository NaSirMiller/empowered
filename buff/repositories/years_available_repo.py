from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import CensusAvailableYear
from buff.utils import get_db_client, get_matching_from_database


class YearsAvailableRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_years(self, dataset_id: int, year: int | None = None) -> list[SQLModel]:
        params = {"dataset_id": dataset_id}
        if year is not None:
            params["year"] = year

        return get_matching_from_database(
            model=CensusAvailableYear,
            params=params,
        )

    def insert_year(self, dataset_id: int, year: int) -> None:
        self.db_client.insert(
            instances=[CensusAvailableYear(id=None, dataset_id=dataset_id, year=year)]
        )
