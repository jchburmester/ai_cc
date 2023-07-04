import numpy as np
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

    def preprocess_data(self):
        df = self.df.copy()
        df = df[df['paper_abstract'].notna()]
        df = df[df['aggregation_type'].isin(['Journal', 'Conference Proceeding'])]
        df['paper_abstract'] = df['paper_abstract'].str.lower()
        df.loc[df['doi'].isna(), 'doi'] = df['doi'].apply(lambda x: str(np.random.randint(1e8)))
        return df
    
    # Extract only abstracts and DOIs
    def abstract_only(self):
        df = self.preprocess_data()
        df = df[['doi', 'paper_abstract']]
        return df
    
    # Returning the dataframe as csv
    def return_csv(self, df, filename):
        df.to_csv(filename, index=False)

    # Split data for later training
    def generate_train_test_split(self, test_size=0.2, random_state=42):
        preprocessed_df = self.preprocess_data()
        # preprocessed_df = self.abstract_only()
        preprocessed_df = shuffle(preprocessed_df, random_state=random_state)
        train_df, test_df = train_test_split(preprocessed_df, test_size=test_size, random_state=random_state)
        return train_df, test_df