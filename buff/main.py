from dotenv import load_dotenv
from getpass import getpass
from logger_setup import set_logger, get_logger
from buff.models.sql.sql_client import SQLClient
import os
import requests


def main():
    set_logger()
    load_dotenv()

    # Call ACS1 (acs_id=1)
    resp = requests.get("http://127.0.0.1:8000/years_available/1")
    print(resp.status_code)
    print(resp.text)


if __name__ == "__main__":
    main()
