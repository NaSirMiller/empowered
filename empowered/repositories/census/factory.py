from empowered.repositories.census.datasets_repo import DatasetRepository
from empowered.repositories.census.estimates_repo import CensusEstimateRepository
from empowered.repositories.census.geography_repo import GeographyRepository
from empowered.repositories.census.groups_repo import GroupsRepository
from empowered.repositories.census.variables_repo import VariablesRepository
from empowered.repositories.census.years_available_repo import YearsAvailableRepository


class RepositoryFactory:
    def __init__(self, client):
        self.client = client

    def dataset(self):
        return DatasetRepository(self.client)

    def group(self):
        return GroupsRepository(self.client)

    def variable(self):
        return VariablesRepository(self.client)

    def estimate(self):
        return CensusEstimateRepository(self.client)

    def geography(self):
        return GeographyRepository(self.client)

    def years(self):
        return YearsAvailableRepository(self.client)
