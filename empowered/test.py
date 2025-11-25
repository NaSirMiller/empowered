from empowered.utils.helpers import get_sql_client
from empowered.models.sql.schemas import CensusMock, CensusEstimate
from empowered.services.census_service import (
    get_counties,
    get_estimates,
    get_groups,
    get_places,
    get_states,
    get_variables,
)

sql_client = get_sql_client()

try:
    sql_client.insert([CensusEstimate(
                        id=None,
                        place_fips=estimate["place_fips"],
                        county_fips=estimate["county_fips"],
                        state_fips=estimate["state_fips"],
                        year_id=year_id,
                        dataset_id=dataset_id,
                        variable_id=estimate["variable"],
                        group_id=estimate["variable"].split("_")[0],
                        estimate=float(estimate["estimate"]),
                        margin_of_error=estimate.get("margin_of_error"),),])
                    
