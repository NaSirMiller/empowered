from pydantic import BaseModel
from enum import Enum

from typing import Optional


class FrequencyEnum(str, Enum):
    ANNUAL = "annual"
    QUINQUENNIAL = "quinquennial"


class DatasetCreate(BaseModel):
    code: str
    frequency: FrequencyEnum


class EstimateCreate(BaseModel):
    geo_id: int
    year_id: int
    dataset_id: int
    variable_id: str
    group_id: str
    estimate: float
    margin_of_error: Optional[float] = None


class PlaceCreate(BaseModel):
    dataset_id: int
    year_id: int
    state_fips: int
    place_fips: int
    place_name: str


class StateCreate(BaseModel):
    dataset_id: int
    year_id: int
    state_fips: int
    state_name: str


class CountyCreate(BaseModel):
    dataset_id: int
    year_id: int
    county_fips: int
    county_name: str


class GroupCreate(BaseModel):
    dataset_id: int
    year_id: int
    group_id: str
    description: str
    variables_count: int


class YearCreate(BaseModel):
    dataset_id: int
    year: int


class VariableCreate(BaseModel):
    dataset_id: int
    year_id: int
    group_id: str
    variable_id: str
    description: Optional[str] = None
