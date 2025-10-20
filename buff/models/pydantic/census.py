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


class StateCreate(BaseModel):
    dataset_id: int
    year_id: int
    state_fips: int
    state_name: str


class CountyCreate(BaseModel):
    dataset_id: int
    year_id: int
    state_fips: int
    county_fips: int
    county_name: str


class CityCreate(BaseModel):
    dataset_id: int
    year_id: int
    state_fips: int
    county_fips: int
    city_fips: int
    city_name: str


class GroupCreate(BaseModel):
    dataset_id: int
    year_id: int
    group_id: str
    description: str
    variables_count: int


class YearCreate(BaseModel):
    dataset_id: int
    year: int
