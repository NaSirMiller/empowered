from pydantic import BaseModel


class YearCreate(BaseModel):
    dataset_id: int
    year: int
