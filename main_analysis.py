from analysis.DatabaseHandler import DatabaseHandler
from analysis.DataGenerator import DataGenerator
from analysis.simple_stats import generate_ngrams, remove_stopwords, remove_grams_with_stopwords
from analysis.utils import df_to_csv
import collections

# Get data from database
db_handler = DatabaseHandler('postgresql://postgres:ai_cc_23@localhost:5432/ai_cc')

column_names = ['doi', 'authors', 'year_of_publication', 'month_of_publication', 'journal',
                'aggregation_type', 'country', 'paper_title', 'paper_abstract', 'cited_by', 'author_keywords']

df = db_handler.get_dataframe_from_table('scopus_data', column_names)

# Get data
data_generator = DataGenerator(df)
df = data_generator.abstract_only()

# Create csv file
df = df_to_csv(df, 'ai_cc_2018_2023.csv')