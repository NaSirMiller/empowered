from sqlmodel import SQLModel

from empowered.utils.logger_setup import get_logger
from empowered.models.sql.schemas import CensusDataset
from empowered.models.sql.sql_client import SQLClient
from empowered.utils.helpers import get_sql_client

logger = get_logger("repo/dataset_repository")


class DatasetRepository:
    def __init__(self, db_client: SQLClient = get_sql_client()) -> None:
        self.db_client = db_client

    def get_by_code(self, code: str) -> list[SQLModel]:
        return self.db_client.select(model=CensusDataset, params={"code": code})

    def insert_code(self, code: str, frequency: int) -> None:
        self.db_client.insert(
            instances=[
                CensusDataset(id=f"{code}{frequency}", code=code, frequency=frequency)
            ]
        )
        logger.info("Successfully inserted new Census code to database")
