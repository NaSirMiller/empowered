from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusEstimate
from empowered.utils import get_db_client
from typing import Optional


class CensusEstimateRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_estimates(
        self,
        place_fips: Optional[int] = None,
        year_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        variable_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> list[SQLModel]:
        """
        Fetch census estimates filtered by any combination of parameters.
        """
        params = {}
        if place_fips is not None:
            params["place_fips"] = place_fips
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
        place_fips: int,
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
            place_fips=place_fips,
            year_id=year_id,
            dataset_id=dataset_id,
            variable_id=variable_id,
            group_id=group_id,
            estimate=estimate,
            margin_of_error=margin_of_error,
        )
        self.db_client.insert(instances=[instance])
