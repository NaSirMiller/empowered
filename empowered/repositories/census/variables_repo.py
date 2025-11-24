from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusVariable
from empowered.utils import get_sql_client


class VariablesRepository:
    def __init__(self, db_client: SQLClient = get_sql_client()) -> None:
        self.db_client = db_client

    def get_variables(
        self,
        dataset_id: int,
        year_id: int,
        group_id: str,
        variable_id: str | None = None,
    ) -> list[SQLModel]:
        parameters = {
            "dataset_id": dataset_id,
            "year_id": year_id,
            "group_id": group_id,
        }
        if variable_id is not None:
            parameters["id"] = variable_id
        return self.db_client.select(model=CensusVariable, params=parameters)

    def insert_variables(
        self,
        variables: list[dict],
        dataset_id: int,
        year_id: int,
        group_id: str,
    ) -> None:
        """
        Insert multiple CensusVariable rows in batch.

        Each item in `variables` must be:
        {
            "variable_id": str,
            "description": str
        }
        """
        instances = [
            CensusVariable(
                id=v["variable_id"],
                dataset_id=dataset_id,
                description=v["description"],
                year_id=year_id,
                group_id=group_id,
            )
            for v in variables
        ]

        return self.db_client.insert(instances=instances)
