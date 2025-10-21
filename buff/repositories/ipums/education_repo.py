from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import IpumsDegreeField, IpumsEducationalAttainment
from buff.utils import get_db_client
from typing import List, Optional


class EducationRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_educational_attainment(
        self, educational_attainment_id: int
    ) -> List[SQLModel]:
        return self.db_client.select(
            model=IpumsEducationalAttainment,
            params={"educational_attainment_id": educational_attainment_id},
        )

    def insert_educational_attainment(
        self, educational_attainment_id: int, level_completed: str
    ) -> None:
        self.db_client.insert(
            IpumsEducationalAttainment(
                educational_attainment_id=educational_attainment_id,
                label=level_completed,
            )
        )

    def get_degree_field(self, degree_field_id: int) -> List[SQLModel]:
        return self.db_client.select(
            model=IpumsDegreeField, params={"degree_field_id": degree_field_id}
        )

    def insert_degree_field(
        self,
        degree_field_id: int,
        degree_type: str,
        is_degree_field2: Optional[bool] = False,
    ) -> None:
        self.db_client.insert(
            IpumsDegreeField(
                degree_field_id=degree_field_id,
                label=degree_type,
                is_degree_field2=is_degree_field2,
            )
        )
