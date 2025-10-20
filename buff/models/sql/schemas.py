from sqlmodel import Field, SQLModel


class CensusDataset(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    code: str = Field(index=True)
    frequency: str


class CensusGroup(SQLModel, table=True):
    id: str = Field(
        default=None,
        primary_key=True,
        description="Group code",
    )
    description: str
    dataset_id: int = Field(foreign_key="CensusDataset.id", index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    variables_count: int


class CensusVariable(SQLModel, table=True):
    id: str = Field(
        default=None,
        primary_key=True,
        description="Variable code of form {group code}_{unique id}",
    )
    description: str
    group_id: str = Field(default=None, foreign_key="CensusGroup.id", index=True)
    dataset_id: int = Field(foreign_key="CensusDataset.id", index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)


class CensusAvailableYear(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    dataset_id: int = Field(foreign_key="CensusDataset.id", index=True)
    year: int = Field(index=True)


class CensusGeography(SQLModel, table=True):
    geo_id: int = Field(default=None, primary_key=True)
    state_fips: int
    state_name: str = Field(index=True)
    county_fips: int
    county_name: str = Field(index=True)
    city_fips: int
    city_name: int = Field(index=True)


class CensusEstimate(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    geo_id: int = Field(foreign_key="CensusGeography.geo_id", index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: int = Field(foreign_key="CensusDataset.id", index=True)
    variable_id: str = Field(foreign_key="CensusVariable.id", index=True)
    group_id: str = Field(foreign_key="CensusGroup.id", index=True)
    estimate: float
    margin_of_error: float | None = Field(default=None)


class IpumsRace(SQLModel, table=True):
    race_id: int = Field(primary_key=True)
    label: str


class IpumsEducationalAttainment(SQLModel, table=True):
    educational_attainment_id: int = Field(primary_key=True)
    label: str


class IpumsDegreeField(SQLModel, table=True):
    degree_field_id: int = Field(primary_key=True)
    label: str
    is_degree_field2: bool = Field(default=None)


class IpumsAnnualIncome(SQLModel, table=True):
    income_id: int = Field(primary_key=True)
    amount: int


class IpumsWageIncome(SQLModel, table=True):
    income_id: int = Field(primary_key=True)
    amount: int


class IpumsCity(SQLModel, table=True):
    city_id: int = Field(primary_key=True)
    name: str
    state_fips_id: int = Field(foreign_key="CensusGeography.state_fips")


class IpumsResponderProfile(SQLModel, table=True):
    responder_id: str = Field(
        primary_key=True, description="Has form {SERIAL}_{PERNUM}"
    )
    race_id: int = Field(foreign_key="IpumsRace.race_id")
    educational_attainment_id: int = Field(
        foreign_key="IpumsEducationalAttainment.educational_attainment_id"
    )
    degree_field_id: int | None = Field(
        default=None, foreign_key="IpumsDegreeField.degree_field_id"
    )
    degreed_field2_id: int | None = Field(
        default=None, foreign_key="IpumsDegreeField.degreed_field_id"
    )
    personal_income_id: int = Field(foreign_key="IpumsIncome.income_id")
    wage_income_id: int = Field(foreign_key="IpumsIncome.income_id")
    live_in_state_id: int = Field(foreign_key="CensusGeography.state_fips")
    state_fips_id: int = Field(foreign_key="IpumsState.state_id")
    city_id: int = Field(foreign_key="IpumsCity.city_id")
