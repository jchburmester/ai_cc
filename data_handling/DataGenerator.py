import numpy as np
import pandas as pd
#from langdetect import detect
#from sklearn import shuffle
#from sklearn.model_selection import train_test_split

'''
def detect_language(text):
    try:
        lang = detect(text)
        return lang == 'en'
    except:
        return False
'''

class DataGenerator:
    def __init__(self, df):
        self.df = df

    def preprocess_data(self, remove_stopwords=False) -> pd.DataFrame:
        df = self.df.copy()
        df = df[df['paper_abstract'].notna()]
        df = df[df['aggregation_type'].isin(['Journal', 'Conference Proceeding'])]
        df['paper_abstract'] = df['paper_abstract'].str.lower()
        df.loc[df['doi'].isna(), 'doi'] = df['doi'].apply(lambda x: str(np.random.randint(1e8)))
        return df
    
    # Extract only abstracts and DOIs
    def abstract_only(self) -> pd.DataFrame:
        df = self.preprocess_data()
        df = df[['doi', 'paper_abstract']]
        return df
    
    # Extract specific columns based on function parameters
    def extract_columns(self, columns: list) -> pd.DataFrame:
        df = self.preprocess_data()
        df = df[columns]
        return df