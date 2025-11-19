from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusVariable
from empowered.utils import get_db_client


class VariablesRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
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

    def insert_variable(
        self,
        dataset_id: int,
        year_id: int,
        group_id: str,
        variable_id: str,
        description: str,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusVariable(
                    id=variable_id,
                    dataset_id=dataset_id,
                    description=description,
                    year_id=year_id,
                    group_id=group_id,
                )
            ]
        )
