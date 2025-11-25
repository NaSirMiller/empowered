from sqlmodel import (
    Field,
    ForeignKeyConstraint,
    SQLModel,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)


class CensusMock(SQLModel, table=True):
    __tablename__ = "CensusMock"
    id: int = Field(primary_key=True, default=None)
    name: str = Field(max_length=255)


class CensusDataset(SQLModel, table=True):
    __tablename__ = "CensusDataset"
    id: str = Field(primary_key=True, max_length=255)
    code: str = Field(index=True, max_length=255)
    frequency: str = Field(max_length=255)


class CensusAvailableYear(SQLModel, table=True):
    __tablename__ = "CensusAvailableYear"
    id: int = Field(default=None, primary_key=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)
    year: int = Field(index=True)

    __table_args__ = (UniqueConstraint("dataset_id", "year", name="uq_dataset_year"),)


class CensusGroup(SQLModel, table=True):
    __tablename__ = "CensusGroup"
    id: str = Field(
        default=None, primary_key=True, max_length=255, description="Group code"
    )
    description: str = Field(max_length=255)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    variables_count: int

    __table_args__ = (UniqueConstraint("dataset_id", "year_id", "id", name="uq_group"),)


class CensusVariable(SQLModel, table=True):
    __tablename__ = "CensusVariable"
    id: str = Field(
        default=None,
        primary_key=True,
        max_length=255,
        description="Variable code {group}_{id}",
    )
    description: str = Field(max_length=255)
    group_id: str = Field(
        default=None, foreign_key="CensusGroup.id", index=True, max_length=255
    )
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)

    __table_args__ = (
        UniqueConstraint("dataset_id", "year_id", "group_id", "id", name="uq_variable"),
    )


class CensusState(SQLModel, table=True):
    __tablename__ = "CensusState"
    state_fips: int = Field()
    state_name: str = Field(index=True, max_length=255)
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)

    __table_args__ = (PrimaryKeyConstraint("state_fips", "dataset_id", "year_id"),)


class CensusCounty(SQLModel, table=True):
    __tablename__ = "CensusCounty"
    county_fips: int = Field()
    county_name: str = Field(index=True, max_length=255)
    state_fips: int = Field()
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)

    __table_args__ = (
        PrimaryKeyConstraint("county_fips", "state_fips", "dataset_id", "year_id"),
    )


class CensusPlace(SQLModel, table=True):
    __tablename__ = "CensusPlace"
    place_fips: int = Field()
    place_name: str = Field(index=True, max_length=255)
    state_fips: int = Field()
    year_id: int = Field(foreign_key="CensusAvailableYear.id", index=True)
    dataset_id: str = Field(foreign_key="CensusDataset.id", index=True, max_length=255)

    __table_args__ = (
        PrimaryKeyConstraint("place_fips", "state_fips", "dataset_id", "year_id"),
    )


class CensusEstimate(SQLModel, table=True):
    __tablename__ = "CensusEstimate"
    place_fips: int
    state_fips: int
    county_fips: int
    year_id: int
    dataset_id: str = Field(max_length=255)
    variable_id: str = Field(max_length=255)
    group_id: str = Field(max_length=255)
    estimate: float
    margin_of_error: float | None = Field(default=None)

    __table_args__ = (
        PrimaryKeyConstraint(
            "place_fips",
            "state_fips",
            "county_fips",
            "dataset_id",
            "year_id",
            "variable_id",
        ),
        ForeignKeyConstraint(
            ["place_fips", "state_fips", "dataset_id", "year_id"],
            [
                "CensusPlace.place_fips",
                "CensusPlace.state_fips",
                "CensusPlace.dataset_id",
                "CensusPlace.year_id",
            ],
        ),
        ForeignKeyConstraint(
            ["state_fips", "dataset_id", "year_id"],
            ["CensusState.state_fips", "CensusState.dataset_id", "CensusState.year_id"],
        ),
        ForeignKeyConstraint(
            ["county_fips", "state_fips", "dataset_id", "year_id"],
            [
                "CensusCounty.county_fips",
                "CensusCounty.state_fips",
                "CensusCounty.dataset_id",
                "CensusCounty.year_id",
            ],
        ),
        ForeignKeyConstraint(
            ["year_id"],
            ["CensusAvailableYear.id"],
        ),
        ForeignKeyConstraint(["dataset_id"], ["CensusDataset.id"]),
        ForeignKeyConstraint(["variable_id"], ["CensusVariable.id"]),
        ForeignKeyConstraint(["group_id"], ["CensusGroup.id"]),
    )


class IngestionCheckpoint(SQLModel, table=True):
    __tablename__ = "IngestionCheckpoint"
    dataset_id: str = Field(primary_key=True, max_length=255)
    year: int = Field(primary_key=True)
    groups_ingested: bool = False
    variables_ingested: bool = False
    geography_ingested: bool = False
    estimates_ingested: bool = False
