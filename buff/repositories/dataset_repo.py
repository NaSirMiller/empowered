from sqlmodel import SQLModel

from buff.logger_setup import get_logger
from buff.models.sql.schemas import CensusDataset
from buff.models.sql.sql_client import SQLClient
from buff.services.utils import get_matching_from_database

logger = get_logger("repo/dataset_repository")


class DatasetRepository:
    def __init__(self, db_client: SQLClient) -> None:
        self.db_client = db_client

    def get_by_code(self, code: str) -> list[SQLModel]:
        dataset_results = get_matching_from_database(
            model=CensusDataset, params={"code": code}
        )
        return dataset_results

    def insert_code(self, code: str, frequency: str) -> None:
        valid_frequencies = {"annual", "quinquennial"}  # every 5 years = quinquennial
        if frequency.lower() not in valid_frequencies:
            raise ValueError(
                f"Invalid frequency '{frequency}'. Must be one of: {', '.join(valid_frequencies)}"
            )
        self.db_client.insert(
            instances=[CensusDataset(id=None, code=code, frequency=frequency)]
        )
        logger.info("Successfully inserted new Census code to database")
