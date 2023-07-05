# [Meta-study:] The involvement of AI in Climate Change: positive or negative impact?
This is a short project that aims to assess the impact of AI and ML on climate change, specifically whether their contributions are primarily positive or negative. The assessment will be done by crawling various scientific journals to gather relevant articles, and then apply clustering and other text analysis techniques on the compiled data.

The goal is to provide a brief, high-level snapshot of how AI and ML intersect with climate change. We'd like to shed light on the dominant topics and views within the scientific community. We're mainly interested in understanding if AI is considered a valuable tool in tackling climate change or, conversely, as an extra factor contributing to the problem.

![Image source: https://hub.jhu.edu/2023/03/07/artificial-intelligence-combat-climate-change/](files/GettyImages-1198212848.jpg)

## Code structure

`main.py` handles and runs all scripts <br />
`BaseScopusCrawler` handles most of the basic Scopus API interaction <br />
`ScopusCrawlerDB` saves the data into an SQLAlchemy database <br />

### Open

- if no doi, look at title, if no title, look at author ids
- filter source type (j, cp) & language (eng) // when ds is generated..
- author citations / references?
- simple lda <br />
- simple ngrams <br />
- doc2vec <br />
- topic modelling (bertopic) + keyword extraction (keybert) <br />
- changepoint analysis

### SQL shorts
```
psql -U postgres -h localhost -p 5432 -f C:\Users\jch\ai_cc\crawling\db_style.sql
```

### Updating environment
```
conda env update --prefix ./myenv --file environment.yml --prune
```

