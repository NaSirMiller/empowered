import requests
from functools import lru_cache
from typing import List, Dict
from buff.logger_setup import get_logger

logger = get_logger(name=__name__)


class IpumsAPIError(Exception):
    pass
