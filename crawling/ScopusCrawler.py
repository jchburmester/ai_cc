from __future__ import annotations

import logging
import requests

logger = logging.getLogger(__name__)
skip_log = logging.getLogger("skipped")


class ScopusCrawler:
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

    def search_articles(self, query, start, count):
        #logger.info(f"Searching for articles with start {start}")
        try:
            response = self._session.get(
                'https://api.elsevier.com/content/search/scopus',
                params={
                    'query': query,
                    'start': start,
                    'count': count,
                    'sort': 'citedby-count',
                    'view': 'COMPLETE'
                }
            )
            #print("Response:", response.text)
            response.raise_for_status() 
            
            return response.json()  
        
        except requests.exceptions.HTTPError as err:
            if response.status_code == 429 and "Quota Exceeded" in response.text:
                logger.warning("Quota exceeded. Rotating key...")
                self.rotate_key()
                return self.search_articles(query, self.count)
            
            elif response.status_code == 400 and "400 Client Error: Bad Request" in response.text:
                skip_log.warning("400 Client Error: Bad Request")
                self.rotate_key()
                return self.search_articles(query, self.count)
            
            else:  
                raise err
        except Exception as err:
            print(response.text)  # Print the response text
            raise err