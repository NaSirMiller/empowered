from empowered.utils.helpers import get_sql_client
from empowered.models.sql.schemas import CensusMock
from empowered.services.census_service import (
    get_counties,
    get_estimates,
    get_groups,
    get_places,
    get_states,
    get_variables,
)

estimates_response = get_estimates(
    acs_id=5,
    year=2019,
    variables=[
        # Population & Households
        "B01003_001E",
        "B25001_001E",
        "B25002_001E",
        "B25010_001E",
    ],
    state_fips=1,
    place_fips=74592,
)

print(estimates_response)
