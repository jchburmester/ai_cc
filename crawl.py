import json
import logging.config
import yaml
import traceback

from crawling.CrawlingHandler import CrawlingHandler

logging.config.fileConfig("log.ini", disable_existing_loggers=False)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    with open(".secrets/keys.json", "r", encoding="utf-8") as f:
        key_data = json.load(f)

    with open("config/keywords_v0.yml", "r", encoding="utf-8") as f:
        keywords = yaml.safe_load(f)

    doc_types = ["ar", "cp"]
    year_range = (1990, 2023) # (both inclusive)

    logger.info(f"Starting search with n={len(keywords)} keywords")

    crawler = CrawlingHandler(key_data['API_Keys'])
    
    try:
        crawler.fetch(keywords, doc_types, year_range)
        print(crawler.n_results)
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e