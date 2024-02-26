import numpy as np
import pandas as pd

class DataGenerator:
    def __init__(self, df):
        self.df = df

    def preprocess_data(self, remove_stopwords=False) -> pd.DataFrame:
        try:
            if isinstance(self.df, tuple):
                df, error_df = self.df
                if not error_df.empty:
                    print("Errors occurred during data extraction. Error rows:")
                    print(error_df)
                    return pd.DataFrame()  # Returning an empty DataFrame if there are errors
            else:
                df = self.df.copy()
            
            print(df['aggregation_type'].unique())
            print("Number of records before preprocessing:", len(df))

            df = df[df['paper_abstract'].notna()]
            print("Number of records after removing NA from paper_abstract:", len(df))

            df = df[df['aggregation_type'].isin(['Journal', 'Conference Proceeding'])]
            print("Number of records after filtering by aggregation_type:", len(df))

            df.loc[df['doi'].isna(), 'doi'] = df['doi'].apply(lambda x: str(np.random.randint(1e8)))
            df['paper_abstract'] = df['paper_abstract'].str.replace('"', '')
            df['paper_abstract'] = df['paper_abstract'].str.replace("'", '')
            df['paper_title'] = df['paper_title'].str.replace('"', '')
            df['paper_title'] = df['paper_title'].str.replace("'", '')
            print("Data preprocessing completed successfully")
            return df
        except Exception as e:
            print("Error occurred during preprocessing of data:", e)
            return pd.DataFrame()

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