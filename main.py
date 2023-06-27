import json
import logging.config

import yaml

from src.ScopusCrawlerDB import ScopusCrawlerDB
from src.classes import NoMoreKeysException
from src.utils import get_all_author_ids_from_db_with_null_data 

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    with open("secrets/keys.json", "r", encoding="utf-8") as f:
        key_data = json.load(f)

    doc_types = ["ar", "cp"]
    with open("crawling/config/keywords_v0.yml", "r", encoding="utf-8") as f:
        keywords = yaml.safe_load(f)

    logger.info(f"Starting search with n={len(keywords)} keywords")

    crawler = ScopusCrawlerDB(key_data)
    # c = ScopusCounter()

    # for k in keywords:
    try:
        for y in range(2000, 2024):
            for papers, query_list in crawler.search_papers(keywords, doc_types, min_year=y, max_year=y, min_pages=-1, intersect_keywords=False, top_n=None, batch_keywords=1):
                if not papers:
                    continue
                logger.debug(f"Found {len(papers)} papers")
                crawler.save_paper_data_from_entries(papers, search_queries=query_list)

                # Check keywords are present for all papers
                # with SafeSession(crawler._db, logger) as session:
                #     all_papers_stmt = text(
                #         "SELECT count(DISTINCT paper_id) FROM papers LEFT JOIN matching USING (paper_id) WHERE keyword_id IS NULL")
                #     q = session.execute(all_papers_stmt).all()
                # q = q[0][0]
                # print(q)
                # assert not q or q == 0, f"No k list: {q}"

    except NoMoreKeysException:
        logger.debug("No more API-keys for paper retrieval. Quota exhausted. Finishing.")