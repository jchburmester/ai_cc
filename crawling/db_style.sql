CREATE DATABASE ai_cc;

\c ai_cc;


CREATE TABLE scopus_data
(
    doi                     TEXT PRIMARY KEY,
    authors                 TEXT[],
    year_of_publication     SMALLINT,
    month_of_publication    SMALLINT,
    journal                 TEXT,
    country                 TEXT,
    paper_title             TEXT,
    paper_abstract          TEXT,
    cited_by                INTEGER,
    author_keywords         TEXT[]
);