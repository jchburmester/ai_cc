CREATE DATABASE scopus;

\c scopus;

CREATE TABLE sources
(
    source_id   TEXT PRIMARY KEY,
    name        TEXT,
    type        TEXT,
    last_update TIMESTAMP DEFAULT now()
);

CREATE TABLE papers
(
    paper_id        TEXT PRIMARY KEY,
    title           TEXT,
    abstract        TEXT,
    year            SMALLINT,
    month           SMALLINT,
    author_keywords TEXT[],
    cited_by        INTEGER,
    data            JSONB,
    source_id       TEXT REFERENCES sources (source_id) ON DELETE SET NULL,
    last_update     TIMESTAMP DEFAULT now()
);

CREATE TABLE authors
(
    author_id   TEXT PRIMARY KEY,
    data        JSONB,
    last_update TIMESTAMP DEFAULT now()
);

CREATE TABLE subject_areas
(
    area_code    CHAR(4) PRIMARY KEY,
    abbreviation VARCHAR(16),
    name         TEXT,
    last_update  TIMESTAMP DEFAULT now()
);

CREATE TABLE authorship
(
    paper_id    TEXT NOT NULL REFERENCES papers (paper_id) ON DELETE CASCADE,
    author_id   TEXT NOT NULL REFERENCES authors (author_id) ON DELETE CASCADE,
    sequence    SMALLINT,
    last_update TIMESTAMP DEFAULT now(),
    PRIMARY KEY (paper_id, author_id)
);

CREATE TABLE classification_sources_sa
(
    source_id   TEXT    NOT NULL REFERENCES sources (source_id) ON DELETE CASCADE,
    area_code   CHAR(4) NOT NULL REFERENCES subject_areas (area_code) ON DELETE CASCADE,
    last_update TIMESTAMP DEFAULT now(),
    PRIMARY KEY (source_id, area_code)
);

CREATE TABLE classification_authors_sa
(
    author_id   TEXT    NOT NULL REFERENCES authors (author_id) ON DELETE CASCADE,
    area_code   CHAR(4) NOT NULL REFERENCES subject_areas (area_code) ON DELETE CASCADE,
    frequency   INTEGER,
    last_update TIMESTAMP DEFAULT now(),
    PRIMARY KEY (author_id, area_code)
);

-- Search keyword tracking

CREATE TABLE search_keywords
(
    keyword_id  SERIAL PRIMARY KEY,
    query       TEXT
        CONSTRAINT no_duplicate_queries UNIQUE NOT NULL,
    last_update TIMESTAMP DEFAULT now()
);

CREATE TABLE matching
(
    paper_id    TEXT    NOT NULL REFERENCES papers (paper_id) ON DELETE CASCADE,
    keyword_id  INTEGER NOT NULL REFERENCES search_keywords (keyword_id) ON DELETE CASCADE,
    last_update TIMESTAMP DEFAULT now(),
    PRIMARY KEY (paper_id, keyword_id)
);