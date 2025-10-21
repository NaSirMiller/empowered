from sqlmodel import SQLModel
from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import IpumsAnnualIncome, IpumsWageIncome
from buff.utils import get_db_client
from typing import List


class IncomeRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_total_income(self, income_id: int) -> List[SQLModel]:
        """Retrieve a total (annual) income record by ID."""
        return self.db_client.select(
            model=IpumsAnnualIncome,
            params={"income_id": income_id},
        )

    def insert_total_income(self, income_id: int, amount: float) -> None:
        """Insert a new total (annual) income record."""
        self.db_client.insert(
            IpumsAnnualIncome(
                income_id=income_id,
                amount=amount,
            )
        )

    def get_wage_income(self, income_id: int) -> List[SQLModel]:
        """Retrieve a wage income record by ID."""
        return self.db_client.select(
            model=IpumsWageIncome,
            params={"income_id": income_id},
        )

    def insert_wage_income(self, income_id: int, amount: float) -> None:
        """Insert a new wage income record."""
        self.db_client.insert(
            IpumsWageIncome(
                income_id=income_id,
                amount=amount,
            )
        )
