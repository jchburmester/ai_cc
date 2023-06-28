from __future__ import annotations

import logging
import traceback
from typing import Generator, Any, List, Dict

import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

from requests import RequestException

from ScopusCrawler import ScopusCrawler


class DBCrawler(ScopusCrawler):
    def __init__(self, scopus_keys: list[str], db_url: str):
        super().__init__(scopus_keys)
        self.db_engine = create_engine(db_url)
        self.metadata = MetaData()

        # Assuming you have a table named 'scopus_data' in your database
        self.table = Table('scopus_data', self.metadata, autoload_with=self.db_engine)

    def write_to_db(self, data: List[Dict[str, Any]]):
        with self.db_engine.connect() as connection:
            connection.execute(self.table.insert(), data)