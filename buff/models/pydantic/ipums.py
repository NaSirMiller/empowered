from pydantic import BaseModel
from typing import Optional


class RaceCreate(BaseModel):
    label: str


class EducationalAttainmentCreate(BaseModel):
    label: str


class DegreeFieldCreate(BaseModel):
    label: str
    is_degree_field2: Optional[bool] = None


class AnnualIncomeCreate(BaseModel):
    amount: int


class WageIncomeCreate(BaseModel):
    amount: int


class CityCreate(BaseModel):
    name: str
    state_fips_id: int


class ResponderProfileCreate(BaseModel):
    responder_id: str  # {SERIAL}_{PERNUM}
    race_id: int
    educational_attainment_id: int
    degree_field_id: Optional[int] = None
    degreed_field2_id: Optional[int] = None
    personal_income_id: int
    wage_income_id: int
    live_in_state_id: int
    state_fips_id: int
    city_id: int
