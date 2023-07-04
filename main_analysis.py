from analysis.DatabaseHandler import DatabaseHandler
from analysis.DataGenerator import DataGenerator
from analysis.simple_stats import generate_ngrams, remove_grams_with_stopwords
import collections

# Get data from database
db_handler = DatabaseHandler('postgresql://postgres:ai_cc_23@localhost:5432/ai_cc')

column_names = ['doi', 'authors', 'year_of_publication', 'month_of_publication', 'journal',
                'aggregation_type', 'country', 'paper_title', 'paper_abstract', 'cited_by', 'author_keywords']

df = db_handler.get_dataframe_from_table('scopus_data', column_names)

# Get data
data_generator = DataGenerator(df)
data = data_generator.abstract_only()

# Extract n-grams for abstracts
bigrams = generate_ngrams(data['paper_abstract'], 2)
bigrams_clean = remove_grams_with_stopwords(bigrams)
trigrams = generate_ngrams(data['paper_abstract'], 3)
fourgrams = generate_ngrams(data['paper_abstract'], 4)

bigrams_freq = collections.Counter(bigrams_clean)
trigrams_freq = collections.Counter(trigrams)
fourgrams_freq = collections.Counter(fourgrams)

print('Bigrams:')
print(bigrams_freq.most_common(10))
print('Trigrams:')
print(trigrams_freq.most_common(10))
print('Fourgrams:')
print(fourgrams_freq.most_common(10))
