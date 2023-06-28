from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import math
import traceback
from typing import Generator

from requests import RequestException
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
from urllib3.exceptions import MaxRetryError, NewConnectionError

from db_wrapper import DBwrapper, SafeSession
from BaseScopusCrawler import BaseScopusCrawler
from classes import FetchException, ScopusCounter



class ScopusCrawlerDB(BaseScopusCrawler):
    def __init__(self, scopus_keys: list[str]):
        self._db = DBwrapper()
        super().__init__(scopus_keys)