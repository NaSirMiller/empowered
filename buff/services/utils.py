from sqlmodel import SQLModel
from typing import Any

def get_from_db(model: SQLModel, params=dict[str, Any]) -> 