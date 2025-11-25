from pydantic import BaseModel, field_validator
from enum import Enum

from typing import List, Optional


class FrequencyEnum(str, Enum):
    ANNUAL = 1
    QUINQUENNIAL = 5


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


class EstimateRequest(BaseModel):
    variables: List[str]
    state: Optional[int] = None
    county: Optional[int] = None
    place: Optional[int] = None

    @field_validator("variables")
    def validate_variables(cls, v):
        if not v:
            raise ValueError("At least one variable must be provided.")
        return v

    @field_validator("place")
    def validate_place_requires_state(cls, v, values):
        if v is not None and values.get("state") is None:
            raise ValueError("Place FIPS requires a state FIPS.")
        return v

    @field_validator("county")
    def validate_county_requires_state(cls, v, values):
        if v is not None and values.get("state") is None:
            raise ValueError("County FIPS requires a state FIPS.")
        return v

    @field_validator("*")
    def validate_geography_exclusive(cls, _, values):
        geo_fields = ["state", "county", "place"]
        provided = [f for f in geo_fields if values.get(f) is not None]

        if len(provided) == 0:
            raise ValueError("Specify a state, county, or place FIPS.")

        if len(provided) > 1:
            raise ValueError("Only one of state, county, or place may be provided.")

        return _
