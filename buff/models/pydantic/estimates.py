from pydantic import BaseModel
from typing import Optional


class EstimateCreate(BaseModel):
    geo_id: int
    year_id: int
    dataset_id: int
    variable_id: str
    group_id: str
    estimate: float
    margin_of_error: Optional[float] = None
