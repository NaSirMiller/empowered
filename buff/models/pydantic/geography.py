from pydantic import BaseModel
from typing import Optional


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
