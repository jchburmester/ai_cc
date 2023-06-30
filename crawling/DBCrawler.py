from __future__ import annotations

import logging
import traceback
import itertools
from typing import Generator, Any, List, Dict

import sqlalchemy
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, func

from requests import RequestException

from .ScopusCrawler import ScopusCrawler
from .db_wrapper import DBwrapper, SafeSession

logger = logging.getLogger(__name__)


class DBCrawler(ScopusCrawler):
    def __init__(self, scopus_keys: list[str]):
        super().__init__(scopus_keys)
        
        self.db_engine = DBwrapper()
        self.metadata = MetaData()
        self.table = Table('scopus_data', self.metadata, autoload_with=self.db_engine)

    def write_to_db(self, data: List[Dict[str, Any]]):
        try:
            # log data with comment
            logger.info(f"Writing data to the database: {data}")
            with self.db_engine.connect() as connection:
                connection.execute(self.table.insert(), [data])
        except sqlalchemy.exc.SQLAlchemyError as e: 
            logger.error(f"Failed to write data to the database: {e}")
            raise

    def _scopus_search(self, keyword: str, doc_type: str, year_range: tuple[int, int], limit: int = None) -> Generator[Dict[str, Any], None, None]:
        page = 1
        count = 1 # limits the number of results per page
        total_results = None
        processed_count = 0

        while total_results is None or page * count < total_results:
            query = f"{keyword} AND DOCTYPE({doc_type})"
            
            result = self.search_articles(query, count)

            if total_results is None:
                total_results = int(result['search-results']['opensearch:totalResults'])

            for article in result['search-results']['entry']:
                yield self.parse_article(article)
                processed_count += 1

                # Check if the limit is reached
                if limit is not None and processed_count >= limit:
                    return

            page += 1

    def fetch(self, keywords: list[str], doc_types: list[str], year_range: tuple[int, int]) -> None:
        limit = 1 # limits the number of articles per keyword and doc_type
        
        for keyword, doc_type in itertools.product(keywords, doc_types):
            logger.info(f"Fetching data for keyword {keyword} and doc_type {doc_type}")
            try:
                record_count = 0
                for record in self._scopus_search(keyword, doc_type, year_range, limit):
                    logger.info("Processing record", extra={'handler': 'progressHandler'})
                    # log record with comment
                    logger.info(f"Processing record: {record}")
                    self.write_to_db(record)
                    record_count += 1
                    logger.info(f"Processed record {record_count}", extra={'handler': 'progressHandler'})

            except RequestException as e:
                logger.error(f"Failed to fetch data for keyword {keyword} and doc_type {doc_type}: {e}")
                continue


    def parse_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and format the relevant fields from a Scopus article
        """
        parsed_article = {}
        errors = list()

        # Parsing with error handling
        # Extract 'authors'
        try:
            parsed_article['authors'] = [author.get('authid') for author in article['author']]
        except (KeyError, TypeError):
            errors.append("title")


        # Extract 'year_of_publication'
        try:
            parsed_article['year_of_publication'] = article['prism:coverDate'][:4]
        except (KeyError, TypeError):
            errors.append("year_of_publication")


        # Extract 'month_of_publication'
        try:
            parsed_article['month_of_publication'] = article['prism:coverDate'][5:7]
        except (KeyError, TypeError):
            errors.append("month_of_publication")


        # Extract 'journal'
        try:
            parsed_article['journal'] = article.get('prism:publicationName')
        except (KeyError, TypeError):
            errors.append("journal")


        # Extract 'country'
        if 'affiliation' in article and len(article['affiliation']) > 0:
            try:
                parsed_article['country'] = article['affiliation'][0].get('affiliation-country')
            except (KeyError, TypeError):
                errors.append("country")


        # Extract 'paper_title'
        try:
            parsed_article['paper_title'] = article.get('dc:title')
        except (KeyError, TypeError):
            errors.append("paper_title")


        # Extract 'paper_abstract'
        try:
            parsed_article['paper_abstract'] = article.get('dc:description')
        except (KeyError, TypeError):
            errors.append("paper_abstract")


        # Extract 'cited_by_count'
        try:
            parsed_article['cited_by_count'] = article.get('citedby-count')
        except (KeyError, TypeError):
            errors.append("cited_by_count")


        # Extract 'keywords'
        try:
            parsed_article['keywords'] = article.get('authkeywords')
        except (KeyError, TypeError):
            errors.append("keywords")

        if errors:
            logger.error(f"Error retrieving '{','.join(errors)}' in article='{article.get('dc:title')}'")

        return parsed_article