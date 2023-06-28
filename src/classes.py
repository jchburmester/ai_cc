import urllib.parse


class ScopusCounter:
    def __init__(self):
        self.filter_na_pages: int = 0
        self.filter_unexpected_page_format: int = 0
        self.filter_skip_count: int = 0
        self.kept_count: int = 0
        self.file_entries: int = 0
        self.all_author_ids: set[str] = set()
        self.all_paper_ids: set[str] = set()
        self.current_status: set[str] = set()


class FetchException(ConnectionRefusedError):
    def __init__(self, code: int, text: str, url: str):
        self.status_code = code
        self.text = text
        self.url = urllib.parse.unquote(url)

    def __str__(self) -> str:
        return f"Returned status code '{self.status_code}' in fetching URL='{self.url}'"

    def __repr__(self) -> str:
        return str(self)


class NoMoreKeysException(Exception):
    pass