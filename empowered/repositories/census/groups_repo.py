from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusGroup
from empowered.utils import get_db_client


class GroupsRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_groups(
        self, dataset_id: int, year_id: int, group_id: str | None = None
    ) -> list[SQLModel]:
        parameters = {"dataset_id": dataset_id, "year_id": year_id}
        if group_id is not None:
            parameters["id"] = group_id
        return self.db_client.select(model=CensusGroup, params=parameters)

    def insert_groups(
        self,
        groups: list[dict],
        dataset_id: int,
        year_id: int,
    ) -> None:
        """
        Insert multiple CensusGroup rows in batch.

        Each item in `groups` must be:
        {
            "group_id": str,
            "description": str,
            "variables_count": int
        }
        """
        instances = [
            CensusGroup(
                id=g["group_id"],
                dataset_id=dataset_id,
                description=g["description"],
                variables_count=g["variables_count"],
                year_id=year_id,
            )
            for g in groups
        ]

        return self.db_client.insert(instances=instances)
