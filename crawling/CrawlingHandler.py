from __future__ import annotations

import logging
import itertools
import sqlalchemy

from typing import Any, List, Dict
from sqlalchemy import insert
from sqlalchemy.sql import select, func
from requests import RequestException

from .BaseCrawler import BaseCrawler
from .db_wrapper import DBwrapper, SafeSession

logger = logging.getLogger(__name__)

logging.getLogger().setLevel(logging.INFO)


class CrawlingHandler(BaseCrawler):

    def __init__(self, scopus_keys: list[str]):
        super().__init__(scopus_keys)

        self.db_engine = DBwrapper()
        self.metadata = self.db_engine.base_type.metadata
        self.table = self.db_engine.get_table('ai_cc_2602')
        self.limit = 100 # limits the number of total articles at one instance of crawling
        self.n_results = []
        self.processed_records = 0

    # Check for existing articles in the database
    def article_exists(self, doi: str, paper_title: str) -> bool:
        with SafeSession(self.db_engine, logger) as session:
            if doi is None or doi == "None":
                # log the processed records
                #logger.info(f"Processed records for keyword {paper_title}: {self.processed_records}")
                select_stmt = select(func.count()).where(
                    (self.table.c.doi.is_(None)) &
                    (self.table.c.paper_title == paper_title)
                )
            else:
                select_stmt = select(func.count()).where(self.table.c.doi == doi)

            result = session.connection().scalar(select_stmt)
            return result > 0

    def write_to_db(self, data: List[Dict[str, Any]]):

        try:
            with SafeSession(self.db_engine, logger) as session:
                #logger.info("Writing data to the database...")

                if not self.article_exists(data['doi'], data['paper_title']):
                    insert_stmt = insert(self.table).values(data)
                    session.connection().execute(insert_stmt)
                else:
                    pass
                    #logger.info(f"Article with DOI {data['doi']} already exists in the database. Skipping insertion.")

        except sqlalchemy.exc.SQLAlchemyError as e:
            logger.error(f"Failed to write data to the database: {e}")
            raise

    def fetch(self, keywords: list[str], doc_types: list[str], year_range: tuple[int, int]) -> None:
        for keyword, doc_type in itertools.product(keywords, doc_types):
            logger.info(f"Fetching data for keyword {keyword}")
            # include this if required:  and doc_type {doc_type} and year range {year_range}

            total_results = 0
            processed_count = 0
            cursor = "*"  # Initial cursor value

            year_param_f = f"(PUBYEAR AFT {year_range[0] - 1} AND PUBYEAR BEF {year_range[1] + 1})" if year_range[0] and year_range[1] else ""
            doc_type_param_f = f"DOCTYPE({doc_type})" if doc_type else ""
            query = f"{keyword} AND {doc_type_param_f}"
            # include this if required:  AND {year_param_f} 

            try:
                result = self.search_articles(query, 0, 25, cursor)
                total_results = int(result['search-results']['opensearch:totalResults'])
                logger.info(f"Total results for query '{query}': {total_results}")
                self.n_results.append(total_results)

                while processed_count < total_results and processed_count < self.limit:
                    for article in result['search-results']['entry']:
                        self.write_to_db(self.parse_article(article))
                        processed_count += 1
                        self.processed_records = processed_count

                    # Update cursor and fetch next page
                    cursor = result["search-results"]["cursor"]["@next"]
                    result = self.search_articles(query, 0, 25, cursor)

            except RequestException as e:
                logger.error(f"Failed to fetch data for keyword {keyword} and doc_type {doc_type}: {e}")
                continue

            logger.info(f"Processed records for keyword {keyword} and doc_type {doc_type}: {self.processed_records}")
            

    def parse_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and format the relevant fields from a Scopus article
        """
        parsed_article = {}
        errors = list()

        # Parsing extracted article data

        # Extract 'DOI'
        try:
            parsed_article['doi'] = article.get('prism:doi')
        except (KeyError, TypeError):
            errors.append("doi")
            

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

        
        # Extract 'aggregation_type'
        try:
            parsed_article['aggregation_type'] = article.get('prism:aggregationType')
        except (KeyError, TypeError):
            errors.append("aggregation_type")


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
            parsed_article['cited_by'] = article.get('citedby-count')
        except (KeyError, TypeError):
            errors.append("cited_by")


        # Extract 'keywords'
        try:
            keywords_string = article.get('authkeywords')  # Get the keywords string
            if keywords_string:
                parsed_article['author_keywords'] = [keyword.strip() for keyword in keywords_string.split('|')]  # Split the string on '|'
        except (KeyError, TypeError):
            errors.append("author_keywords")

        if errors:
            logger.error(f"Error retrieving '{','.join(errors)}' in article='{article.get('dc:title')}'")

        return parsed_article