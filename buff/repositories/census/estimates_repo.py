from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import CensusEstimate
from buff.utils import get_db_client
from typing import Optional


class CensusEstimateRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_estimates(
        self,
        geo_id: Optional[int] = None,
        year_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        variable_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> list[SQLModel]:
        """
        Fetch census estimates filtered by any combination of parameters.
        """
        params = {}
        if geo_id is not None:
            params["geo_id"] = geo_id
        if year_id is not None:
            params["year_id"] = year_id
        if dataset_id is not None:
            params["dataset_id"] = dataset_id
        if variable_id is not None:
            params["variable_id"] = variable_id
        if group_id is not None:
            params["group_id"] = group_id

        return self.db_client.select(model=CensusEstimate, params=params)

    def insert_estimate(
        self,
        geo_id: int,
        year_id: int,
        dataset_id: int,
        variable_id: str,
        group_id: str,
        estimate: float,
        margin_of_error: float | None = None,
    ) -> None:
        """
        Insert a single CensusEstimate record.
        """
        instance = CensusEstimate(
            id=None,
            geo_id=geo_id,
            year_id=year_id,
            dataset_id=dataset_id,
            variable_id=variable_id,
            group_id=group_id,
            estimate=estimate,
            margin_of_error=margin_of_error,
        )
        self.db_client.insert(instances=[instance])
