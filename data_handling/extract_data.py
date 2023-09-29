from DatabaseHandler import DatabaseHandler
from DataGenerator import DataGenerator
import os

# Returning a dataframe as csv
def df_to_csv(df, filename):
    dir = 'files'

    if not os.path.exists(dir):
        os.makedirs(dir)

    file_path = os.path.join(dir, filename)
    df.to_csv(file_path, index=False)

# Get data from database
db_handler = DatabaseHandler('postgresql://postgres:ai_cc_23@localhost:5432/ai_cc')

column_names = ['doi', 'authors', 'year_of_publication', 'month_of_publication', 'journal',
                'aggregation_type', 'country', 'paper_title', 'paper_abstract', 'cited_by', 'author_keywords']

df = db_handler.get_dataframe_from_table('ai_cc_2607', column_names)

# Get data
data_generator = DataGenerator(df)
df = data_generator.preprocess_data()

# Create csv file
df = df_to_csv(df, 'ai_cc_2607.csv')