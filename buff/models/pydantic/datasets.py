from pydantic import BaseModel
from enum import Enum


class FrequencyEnum(str, Enum):
    ANNUAL = "annual"
    QUINQUENNIAL = "quinquennial"


class DatasetCreate(BaseModel):
    code: str
    frequency: FrequencyEnum
