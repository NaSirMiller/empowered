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

    def insert_group(
        self,
        dataset_id: int,
        year_id: int,
        group_id: str,
        description: str,
        variables_count: int,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusGroup(
                    id=group_id,
                    dataset_id=dataset_id,
                    description=description,
                    variables_count=variables_count,
                    year_id=year_id,
                )
            ]
        )
