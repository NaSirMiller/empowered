from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusEstimate
from empowered.utils.helpers import get_sql_client
from empowered.utils.logger_setup import get_logger
from typing import List, Optional


logger = get_logger(__name__)


class CensusEstimateRepository:
    def __init__(self, db_client: SQLClient = get_sql_client()) -> None:
        self.db_client = db_client

    def get_estimates(
        self,
        place_fips: Optional[int] = None,
        year_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        variable_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> list[dict]:
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
        results = self.db_client.select(model=CensusEstimate, filters=params)
        return [r.model_dump() for r in results]

    def insert_estimates(
        self,
        year_id: int,
        dataset_id: int,
        estimates: List[
            dict
        ],  # each dict: {"variable": str, "value": float, "margin_of_error": Optional[float]}
    ) -> None:
        instances = []
        for estimate in estimates:
            try:
                instances.append(
                    CensusEstimate(
                        id=None,
                        place_fips=estimate["place_fips"],
                        county_fips=estimate["county_fips"],
                        state_fips=estimate["state_fips"],
                        year_id=year_id,
                        dataset_id=dataset_id,
                        variable_id=estimate["variable"],
                        group_id=estimate["variable"].split("_")[0],
                        estimate=float(estimate["estimate"]),
                        margin_of_error=estimate.get("margin_of_error"),
                    )
                )
            except:
                logger.info(f"Errored estimate: {estimate}")
        if instances:
            self.db_client.insert(instances=instances)
