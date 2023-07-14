from __future__ import annotations

import logging
import requests

from requests import RequestException

logger = logging.getLogger(__name__)
skip_log = logging.getLogger("skipped")


class BaseCrawler:
    def __init__(self, scopus_keys):
        self._keys = scopus_keys
        self._key_index = 0
        self._session = requests.Session()
        self._session.headers.update({
            'Accept': 'application/json',
            'X-ELS-APIKey': self._keys[self._key_index]
        })
        #self.search_params = {'query': {}, 'start': 0, 'count': 25}

    def rotate_key(self):
        self._key_index = (self._key_index + 1) % len(self._keys)
        self._session.headers.update({
            'X-ELS-APIKey': self._keys[self._key_index]
        })

    def search_articles(self, query, start, count, cursor):
        #logger.info(f"Searching for articles with start {start}")
        try:
            
            response = self._session.get(
                'https://api.elsevier.com/content/search/scopus',
                params={
                    'query': query,
                    'cursor': cursor,
                    'start': start,
                    'count': count,
                    'sort': 'citedby-count',
                    'view': 'COMPLETE'
                }
            )

            remaining_quota = int(response.headers.get("x-ratelimit-remaining", -1))
            total_quota = int(response.headers.get("x-ratelimit-limit", -1))
            #logger.debug(f"Quota: {remaining_quota} / {total_quota}")
            response.raise_for_status() 
            
            return response.json()  
        
        except RequestException as err:
            next_cursor: str = response["search-results"]["cursor"]["@next"]
            return self.search_articles(query, start, count, next_cursor)

        except Exception as err:
            print(response.text)
            raise err