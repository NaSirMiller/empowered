from sqlmodel import select
from empowered.models.sql import IngestionCheckpoint
from empowered.models.sql.sql_client import SQLClient
from empowered.utils.helpers import get_sql_client


class CheckpointRepository:
    def __init__(self, db_client: SQLClient = get_sql_client()):
        self.db_client = db_client

    def get_or_create(self, dataset_id: str, year: int) -> dict:
        with self.db_client.session_scope() as session:
            stmt = select(IngestionCheckpoint).where(
                IngestionCheckpoint.dataset_id == dataset_id,
                IngestionCheckpoint.year == year,
            )
            checkpoint = session.exec(stmt).first()

            if checkpoint is None:
                checkpoint = IngestionCheckpoint(dataset_id=dataset_id, year=year)
                session.add(checkpoint)
                session.commit()
                session.refresh(checkpoint)

            return checkpoint.model_dump()

    def mark_completed(self, dataset_id: str, year: int, field: str):
        assert field in {
            "groups_ingested",
            "variables_ingested",
            "geography_ingested",
            "estimates_ingested",
        }, f"Invalid checkpoint field: {field}"

        with self.db_client.session_scope() as session:
            stmt = select(IngestionCheckpoint).where(
                IngestionCheckpoint.dataset_id == dataset_id,
                IngestionCheckpoint.year == year,
            )
            checkpoint = session.exec(stmt).first()

            if checkpoint is None:
                checkpoint = IngestionCheckpoint(dataset_id=dataset_id, year=year)
                session.add(checkpoint)
                session.commit()
                session.refresh(checkpoint)

            setattr(checkpoint, field, True)
            session.add(checkpoint)
            session.commit()
            session.refresh(checkpoint)
