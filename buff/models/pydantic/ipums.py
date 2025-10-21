from pydantic import BaseModel
from typing import Optional


class RaceCreate(BaseModel):
    race_id: int
    label: str


class EducationalAttainmentCreate(BaseModel):
    educational_attainment_id: int
    label: str


class DegreeFieldCreate(BaseModel):
    degree_field_id: int
    label: str
    is_degree_field2: Optional[bool] = None


class AnnualIncomeCreate(BaseModel):
    income_id: int
    amount: int


class WageIncomeCreate(BaseModel):
    income_id: int
    amount: int


class CityCreate(BaseModel):
    city_id: int
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
