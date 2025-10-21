from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import IpumsRace
from buff.utils import get_db_client
from typing import List


class DemographicsRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_race_name(self, race_id: int) -> List[SQLModel]:
        return self.db_client.select(model=IpumsRace, params={"race_id": race_id})

    def insert_race(self, race_id: int, race_name: str) -> None:
        self.db_client.insert(IpumsRace(race_id=race_id, label=race_name))
