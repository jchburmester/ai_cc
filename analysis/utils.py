import os
import pandas as pd


# Returning a dataframe as csv
def df_to_csv(df, filename):
    dir = 'files'

    if not os.path.exists(dir):
        os.makedirs(dir)

    file_path = os.path.join(dir, filename)
    df.to_csv(file_path, index=False)