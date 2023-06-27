from __future__ import annotations

import abc
import json
import logging
import threading
import time
import traceback
from pathlib import Path
from typing import Any

import requests
from requests import Session
from tqdm import tqdm
from urllib3.exceptions import MaxRetryError, NewConnectionError
from requests.exceptions import RequestException

from classes import NoMoreKeysException, FetchException, ScopusCounter
from utils import create_req_session, non_blocking_lock
import datetime as dt

logger = logging.getLogger(__name__)
skip_log = logging.getLogger("skipped")


class BaseScopusCrawler(abc.ABC):
    def __init__(self, scopus_keys: list[str], out_path: Path = None):
        self._keys: list[str] = scopus_keys

        self._current_keys: dict[str, int | None] = {
            "author": None,
            "paper_search": None,
            "abstract": None
        }

        impossibly_high_date = int(dt.datetime(2100, 12, 25).strftime('%s'))
        self._reset_timestamp: dict[str, int] = {
            "author": impossibly_high_date,
            "paper_search": impossibly_high_date,
            "abstract": impossibly_high_date
        }

        self._base_url_search = "https://api.elsevier.com/content/search/scopus"
        self._base_url_authors = "https://api.elsevier.com/content/author/author_id"
        self._base_url_abstract = "https://api.elsevier.com/content/abstract/scopus_id"

        if out_path:
            self._results_path = out_path
            self._results_path.mkdir(parents=True, exist_ok=True)
            self._enc = dict(encoding="utf-8")

        self._headers = {
            "X-ELS-ResourceVersion": "new",
            "Accept": "application/json"
        }

        # Potentially useful for future multithread usage
        self._thread_locks = {
            "author": threading.Lock(),
            "paper_search": threading.Lock(),
            "abstract": threading.Lock()
        }

        self._get_new_key("author")
        self._get_new_key("paper_search")
        self._get_new_key("abstract")

        # See https://dev.elsevier.com/api_key_settings.html for rate limits
        self._paper_session = create_req_session(per_second=8)
        self._author_session = create_req_session(per_second=2)

    def print_reset_dates(self) -> None:
        for k, v in self._reset_timestamp.items():
            logger.info(f"First reset date for service '{k}': {dt.datetime.fromtimestamp(v).isoformat()}")

    def _get_new_key(self, service: str) -> None:
        """
        Get a new key from the list for 'service'.
        Function is thread safe. Max 1 thread will change this at a time for each service.

        :param service: a string with the name of the service
        """
        with non_blocking_lock(self._thread_locks[service]) as locked:
            # WARNING: this could create infinite recursion in special cases
            if locked:
                cur_index = self._current_keys[service]
                if cur_index is None:
                    index = 0
                else:
                    index = cur_index + 1
                self._current_keys[service] = index
                self._headers["X-ELS-APIKey"] = self._keys[index]
                logger.info(f"Using new key for service '{service}': {index} - {self._keys[index]}")
                time.sleep(1)

    def __make_rec_request(self, service: str, session: Session, url: str, params: list) -> requests.Response:
        response = session.get(url, headers=self._headers, params=params, timeout=90)
        remaining_quota = int(response.headers.get("x-ratelimit-remaining", -1))
        total_quota = int(response.headers.get("x-ratelimit-limit", -1))
        logger.debug(f"Quota: {remaining_quota} / {total_quota}")

        if response.status_code == 429:
            status_quota = response.headers.get("x-els-status", None)
            reset_time = int(response.headers.get("x-ratelimit-reset", -1))
            date_reset = dt.datetime.fromtimestamp(reset_time).isoformat()
            if (status_quota and isinstance(status_quota, str) and status_quota.strip() == "QUOTA_EXCEEDED") or remaining_quota == 0:
                logger.warning(f"Quota exhausted for service={service}, {remaining_quota}/{total_quota} calls, reset on {date_reset}")
                # Save the minimum date where a reset occur
                self._reset_timestamp[service] = min(reset_time, self._reset_timestamp[service])
                try:
                    # WARN: potential infinite recursion when get_new_key is never entered because of lock
                    self._get_new_key(service)
                except IndexError:
                    logger.critical(f"Keys exhausted for service={service}, header={json.dumps(dict(response.headers), indent=2)}\n{traceback.format_exc()}")
                    raise NoMoreKeysException()
                # Retry request
                response = self.__make_rec_request(service, session, url, params)
        return response

    def _make_request(self, service: str, session: Session, url: str, params: list[tuple[str, Any]] | None = None) -> dict:
        """
        Utility to make general Scopus requests

        :param service: API service in use
        :param session: requests' session to use
        :param url: base url to fetch
        :param params: query parameters to add
        :raise FetchException if status code is different from 200
        :return: JSON resulting from call. May rise JSON decode exception if output is not in right format
        """
        response = self.__make_rec_request(service, session, url, params)
        if not response.status_code == 200:
            t = response.text
            raise FetchException(response.status_code, t, response.url)
        page = response.json()
        return page

    @staticmethod
    def _filter_page_number(entries: list[dict[str, Any]], min_p: int) -> tuple[list[dict[str, Any]], int, int, int]:
        keep = list()
        unexpected = 0
        skipped_count = 0
        na_count = 0
        for e in entries:
            pr: str | None = e["prism:pageRange"]  # "247-253"
            doi: str | None = e.get("prism:doi", None)
            if pr is not None:
                try:
                    [first, last] = pr.split("-")
                    first = int(first.strip())
                    last = int(last.strip())
                    n_pages: int = last - first + 1
                    if n_pages >= min_p:
                        keep.append(e)
                    else:
                        logger.debug(f"Skipped entry {doi} with {n_pages} - skip count={skipped_count}")
                        skipped_count += 1
                except Exception:
                    keep.append(e)
                    unexpected += 1
                    logger.warning(f"Page range check went astray, adding entry {doi} with page '{pr}'")
            else:
                na_count += 1
                keep.append(e)

        return keep, unexpected, na_count, skipped_count

    def get_abstract_data(self, scopus_ids: list[str], c: ScopusCounter = None) -> dict[str, Any]:
        """
        Retrieve full abstract data from selected scopus IDs.
        Abstracts that resolve to a connection error are skipped.

        :param scopus_ids: list of IDs to retrieve
        :return: key value object as {scopus_ids -> JSON abstract response}
        """
        data = dict()
        params = [("view", "FULL")]
        for s_id in scopus_ids:
            api_resource = f"{self._base_url_abstract}/{s_id}"
            try:
                page = self._make_request("abstract", self._paper_session, url=api_resource, params=params)
            except FetchException as e:
                logger.error(f"Scopus returned response status {e.status_code} on url='{e.url}': {e.text}. Fetching abstract={s_id}. Skipping.")
                skip_log.info(f"fetch {e.status_code} - abstract={s_id}, url='{e.url}'")
                if c:
                    c.filter_skip_count += 1
                continue
            except (MaxRetryError, ConnectionError, NewConnectionError, RequestException):
                logger.error(f"Max retry/connection error on abstract='{s_id}'. Skipping.")
                skip_log.info(f"connection - abstract={s_id}")
                if c:
                    c.filter_skip_count += 1
                continue

            try:
                abs_data = page["abstracts-retrieval-response"]
                data[s_id] = abs_data
            except (KeyError, IndexError, TypeError):
                logger.error(f"Abstract='{s_id}' data are in unexpected format: {traceback.format_exc()}")
                skip_log.info(f"missing data - abstract={s_id}")
                if c:
                    c.filter_unexpected_page_format += 1
                continue
        return data

    def get_author_data(self, scopus_ids: list[str], c: ScopusCounter = None) -> dict[str, Any]:
        """
        Retrieve full author data from selected scopus author IDs.
        Authors that resolve to a connection error are skipped.

        :param scopus_ids: list of IDs to retrieve
        :return: key value object as {scopus_ids -> JSON author response}
        """

        data = dict()
        params = [("view", "ENHANCED")]
        for a_id in tqdm(scopus_ids):
            api_resource = f"{self._base_url_authors}/{a_id}"
            try:
                page = self._make_request("author", self._author_session, url=api_resource, params=params)
            except FetchException as e:
                logger.error(f"Scopus returned response status {e.status_code} on url='{e.url}': {e.text}. Fetching author='{a_id}'. Skipping.")
                skip_log.info(f"author={a_id}")
                if c:
                    c.filter_skip_count += 1
                continue
            except (MaxRetryError, ConnectionError, NewConnectionError, RequestException):
                logger.error(f"Max retry/connection error on author='{a_id}'. Skipping.")
                skip_log.info(f"author={a_id}")
                if c:
                    c.filter_skip_count += 1
                continue

            try:
                author_data = page["author-retrieval-response"][0]
                data[a_id] = author_data
            except KeyError:
                logger.error(f"Author='{a_id}' data are in unexpected format (KeyError): {traceback.format_exc()}")
                skip_log.info(f"author={a_id}")
                if c:
                    c.filter_unexpected_page_format += 1
                continue
            except IndexError:
                logger.error(f"Author='{a_id}' data are in unexpected format (IndexError): {traceback.format_exc()}")
                skip_log.info(f"author={a_id}")
                if c:
                    c.filter_unexpected_page_format += 1
                continue
        return data