from __future__ import annotations

import logging
import traceback
import yaml
import itertools
from typing import Generator, Any, List, Dict

import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

from requests import RequestException

from ScopusCrawler import ScopusCrawler

logger = logging.getLogger(__name__)


class DBCrawler(ScopusCrawler):
    def __init__(self, scopus_keys: list[str]):
        super().__init__(scopus_keys)
        
        with open("db_config.yml", "r") as f:
            conf = yaml.safe_load(f)

        db_url = f"postgresql+psycopg2://{conf['PG_USER']}:{conf['PG_PASSWORD']}@{conf['PG_HOST']}:{conf['PG_PORT']}/{conf['PG_DATABASE']}"
        
        self.db_engine = create_engine(db_url)
        self.metadata = MetaData()
        self.table = Table('scopus_data', self.metadata, autoload_with=self.db_engine)

    def write_to_db(self, data: List[Dict[str, Any]]):
        try:
            with self.db_engine.connect() as connection:
                connection.execute(self.table.insert(), data)
        except sqlalchemy.exc.SQLAlchemyError as e:
            logger.error(f"Failed to write data to the database: {e}")
            raise

    def _scopus_search(self, keyword: str, doc_type: str) -> Generator[Dict[str, Any], None, None]:
        page = 1
        count = 100
        total_results = None

        while total_results is None or page * count < total_results:
            result = self.search_articles(f"{keyword} AND DOCTYPE({doc_type})", count)

            if total_results is None:
                total_results = int(result['search-results']['opensearch:totalResults'])

            for article in result['search-results']['entry']:
                yield self.parse_article(article)

            page += 1

    def fetch(self, keywords: list[str], doc_types: list[str], year_range: tuple[int, int]) -> None:
        for keyword, doc_type in itertools.product(keywords, doc_types):
            logger.info(f"Fetching data for keyword {keyword} and doc_type {doc_type}")
            try:
                for record in self._scopus_search(keyword, doc_type):
                    logger.info("Processing record", extra={'handler': 'progressHandler'})
                    self.write_to_db(record)
                    logger.info("Data written to the database", extra={'handler': 'progressHandler'})
            except RequestException as e:
                logger.error(f"Failed to fetch data for keyword {keyword} and doc_type {doc_type}: {e}")
                continue

    def parse_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and format the relevant fields from a Scopus article
        """
        parsed_article = {}

        # Extract 'authors' field if present
        if 'author' in article:
            parsed_article['authors'] = [author['ce:indexed-name'] for author in article['author']]

        # Extract 'year_of_publication' field if present
        if 'prism:coverDate' in article:
            parsed_article['year_of_publication'] = article['prism:coverDate'][:4]  # Assuming date format is YYYY-MM-DD

        # Extract 'journal' field if present
        parsed_article['journal'] = article.get('prism:publicationName')

        # Extract 'country' field if present
        if 'affiliation' in article and len(article['affiliation']) > 0:
            parsed_article['country'] = article['affiliation'][0].get('affiliation-country')

        # Extract 'paper_title' field if present
        parsed_article['paper_title'] = article.get('dc:title')

        # Extract 'paper_abstract' field if present
        parsed_article['paper_abstract'] = article.get('dc:description')

        # Extract 'keywords' field if present
        parsed_article['keywords'] = article.get('authkeywords')

        # Extract 'subject_area' field if present
        if 'subject-area' in article:
            parsed_article['subject_area'] = [subject['$'] for subject in article['subject-area']]

        return parsed_article
