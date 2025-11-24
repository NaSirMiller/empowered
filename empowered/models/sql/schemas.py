from sqlmodel import Field, PrimaryKeyConstraint, SQLModel


class CensusDataset(SQLModel, table=True):
    id: str = Field(primary_key=True)
    code: str = Field(index=True)
    frequency: str


class CensusGroup(SQLModel, table=True):
    id: str = Field(
        default=None,
        primary_key=True,
        description="Group code",
    )
    description: str
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)
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
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)


class CensusAvailableYear(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)
    year: int = Field(index=True)


class CensusState(SQLModel, table=True):
    state_fips: int = Field(primary_key=True)
    state_name: str = Field(index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)


class CensusCounty(SQLModel, table=True):
    county_fips: int = Field(primary_key=True)
    county_name: str = Field(index=True, foreign_key="CensusState.state_fips")
    state_fips: int = Field(primary_key=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)

    __table_args__ = (PrimaryKeyConstraint("county_fips", "state_fips"),)


class CensusPlace(SQLModel, table=True):
    place_fips: int = Field()
    place_name: int = Field(index=True)
    state_fips: int
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)

    __table_args__ = (PrimaryKeyConstraint("place_fips", "state_fips"),)


class CensusEstimate(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    place_fips: int = Field(foreign_key="CensusPlace.place_fips", index=True)
    state_fips: int = Field(foreign_key="CensusState.state_fips", index=True)
    county_fips: int = Field(foreign_key="CensusCounty.county_fips", index=True)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True)
    variable_id: str = Field(foreign_key="CensusVariable.id", index=True)
    group_id: str = Field(foreign_key="CensusGroup.id", index=True)
    estimate: float
    margin_of_error: float | None = Field(default=None)
