import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

import pandas as pd

engine = sqlalchemy.create_engine('postgresql://postgres:ai_cc_23@localhost:5432/ai_cc')

Session = sessionmaker(bind=engine)
session = Session()

inspector = inspect(engine)
table_names = inspector.get_table_names()

metadata = sqlalchemy.MetaData()
metadata.reflect(bind=engine, views=True)

scopus_data = metadata.tables['scopus_data']

query = session.query(scopus_data).all()

# make list to pandas dataframe; using the following column names:
#     doi, authors, year_of_publication, month_of_publication, journal, aggregation_type, country, paper_title, paper_abstract, cited_by, author_keywords

df = pd.DataFrame(query, columns=['doi', 'authors', 'year_of_publication', 'month_of_publication', 'journal', 'aggregation_type', 'country', 'paper_title', 'paper_abstract', 'cited_by', 'author_keywords'])

# do some preprocessing on the dataframe
# assign a randomly generated id to rows that have a null doi
# remove colums except doi and abstract
# remove rows that have a null abstract
# remove rows that are not in english
# remove rows that have not cp or j as aggregation_type
# small caps all abstracts

def preprocessing(df):
    df = df[df['paper_abstract'].notna()]
    df = df[df['aggregation_type'].isin(['cp', 'j'])]
    df = df[['id', 'paper_abstract']]
    df = df[df['paper_abstract'].str.contains('english', case=False)]
    df['paper_abstract'] = df['paper_abstract'].str.lower()
    df['doi'] = df['doi'].fillna('no_doi')
    df['id'] = df['doi'].apply(lambda x: hash(x))
    return df



session.close()