from typing import Any, Dict, List, Type, Optional
from sqlmodel import SQLModel, Session, create_engine, select, text
from logger_setup import get_logger

logger = get_logger(__name__)


class SQLClient:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str,
        echo: bool = False,
    ) -> None:
        """Initialize the SQL client and create all defined tables."""
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
        try:
            logger.info("Creating engine through sqlmodel...")
            self.engine = create_engine(connection_string, echo=echo)
            logger.info(f"Engine created.")
            logger.info("Connecting to SQL server...")
            logger.info("Creating tables defined in project...")
            SQLModel.metadata.create_all(self.engine)
            logger.info("Tables created successfully.")
            logger.info("Connection successful.")
        except Exception as e:
            logger.exception(f"Error connecting to SQL Server:\n{server} - {e}")
            raise e

    # Generic execute for raw SQL
    def execute(
        self, statement: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Execute a raw SQL statement and return a list of results (SQLModel objects or tuples)."""
        parameters = parameters or {}
        with Session(self.engine) as session:
            try:
                stmt = text(statement)
                result = session.exec(stmt, params=parameters)
                rows = result.all()
                session.commit()
                return rows
            except Exception as e:
                logger.exception(f"Error executing statement: {statement}")
                raise

    # Insert one or many SQLModel instances
    def insert(self, instances: List[SQLModel]) -> None:
        if not instances:
            return
        with Session(self.engine) as session:
            try:
                session.add_all(instances)
                session.commit()
                for instance in instances:
                    session.refresh(instance)
            except Exception as e:
                logger.exception("Error inserting data")
                raise

    # Update objects using SQLModel (pass a dictionary of changes)
    def update(
        self, model: Type[SQLModel], where: Dict[str, Any], updates: Dict[str, Any]
    ) -> List[SQLModel]:
        """Update rows and return the updated SQLModel objects."""
        updated_rows = []
        with Session(self.engine) as session:
            stmt = select(model)
            for attr, value in where.items():
                stmt = stmt.where(getattr(model, attr) == value)
            results = session.exec(stmt).all()
            for row in results:
                for attr, value in updates.items():
                    setattr(row, attr, value)
                updated_rows.append(row)
            session.commit()
        return updated_rows

    # Select using SQLModel
    def select(
        self,
        model: Type[SQLModel],
        filters: Optional[Dict[str, Any]] = None,
        group_by: Optional[List[Any]] = None,
        order_by: Optional[List[Any]] = None,
    ) -> List[SQLModel]:
        """Return a list of SQLModel objects matching the query."""
        with Session(self.engine) as session:
            stmt = select(model)
            filters = filters or {}
            for attr, value in filters.items():
                stmt = stmt.where(getattr(model, attr) == value)
            if group_by:
                stmt = stmt.group_by(*group_by)
            if order_by:
                stmt = stmt.order_by(*order_by)

            result = session.exec(stmt).all()
            return result
