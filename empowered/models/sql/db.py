import os
from dotenv import load_dotenv

from empowered.models.sql.sql_client import SQLClient

load_dotenv()

db_client: SQLClient = SQLClient(
    server=os.getenv("SQL_SERVER"),
    database=os.getenv("SQL_DATABASE"),
    username=os.getenv("SQL_USERNAME"),
    driver=os.getenv("SQL_DRIVER").replace(" ", "+"),
    password=os.getenv("SQL_PASSWORD"),
)
