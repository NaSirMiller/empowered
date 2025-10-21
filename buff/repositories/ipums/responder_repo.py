from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import IpumsResponderProfile
from buff.utils import get_db_client
from typing import List, Optional


class ResponderProfileRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_responder(self, responder_id: str) -> List[SQLModel]:
        return self.db_client.select(
            model=IpumsResponderProfile, params={"responder_id": responder_id}
        )

    def insert_responder(
        self,
        responder_id: str,
        race_id: int,
        educational_attainment_id: int,
        degree_field_id: Optional[int],
        is_degree_field: bool,
        personal_income_id: int,
        wage_income_id: int,
        live_in_state_id: int,
        state_fips_id: int,
        city_id: int,
    ) -> None:
        self.db_client.insert(
            IpumsResponderProfile(
                responder_id=responder_id,
                race_id=race_id,
                educational_attainment_id=educational_attainment_id,
                degree_field_id=degree_field_id,
                degreed_field2_id=is_degree_field if is_degree_field else None,
                personal_income_id=personal_income_id,
                wage_income_id=wage_income_id,
                live_in_state_id=live_in_state_id,
                state_fips_id=state_fips_id,
                city_id=city_id,
            )
        )
