from pydantic import BaseModel


class GroupCreate(BaseModel):
    dataset_id: int
    year_id: int
    group_id: str
    description: str
    variables_count: int
