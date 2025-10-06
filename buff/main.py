from dotenv import load_dotenv
from getpass import getpass
from logger_setup import set_logger, get_logger
from models.sql_client import SQLClient
import os
import requests


def main():
    set_logger()

    # Call ACS1 (acs_id=1)

    resp = requests.get("http://127.0.0.1:8000/groups_available/1/2008")
    print(resp.status_code)
    print(resp.text)


if __name__ == "__main__":
    main()
