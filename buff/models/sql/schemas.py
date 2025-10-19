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
