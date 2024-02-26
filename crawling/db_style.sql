CREATE DATABASE ai_cc;

\c ai_cc;


CREATE TABLE ai_cc_2602
(
    doi                     TEXT,
    authors                 TEXT[],
    year_of_publication     SMALLINT,
    month_of_publication    SMALLINT,
    journal                 TEXT,
    aggregation_type        TEXT,
    country                 TEXT,
    paper_title             TEXT,
    paper_abstract          TEXT,
    cited_by                INTEGER,
    author_keywords         TEXT[]
);