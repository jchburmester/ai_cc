import json
import logging.config

import yaml

from src.ScopusCrawlerDB import ScopusCrawlerDB
from src.classes import NoMoreKeysException

logging.config.fileConfig("log.ini", disable_existing_loggers=False)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    with open("secrets/keys.json", "r", encoding="utf-8") as f:
        key_data = json.load(f)

    doc_types = ["ar", "cp"]
    with open("crawling/config/keywords_v0.yml", "r", encoding="utf-8") as f:
        keywords = yaml.safe_load(f)

    logger.info(f"Starting search with n={len(keywords)} keywords")

    crawler = ScopusCrawlerDB(key_data['API_Keys'])

    # for k in keywords:
    try:
        # start with 2024 to ensure shorter list for testing
        for y in range(2024, 2024):
            for papers, query_list in crawler.search_papers(keywords, doc_types, min_year=y, max_year=y, min_pages=-1, intersect_keywords=False, top_n=None, batch_keywords=1):
                if not papers:
                    continue
                logger.debug(f"Found {len(papers)} papers")
                crawler.save_paper_data_from_entries(papers, search_queries=query_list)

    except NoMoreKeysException:
        logger.debug("No more API-keys for paper retrieval. Quota exhausted. Finishing.")

    # See when it's possible to restart it
    crawler.print_reset_dates()