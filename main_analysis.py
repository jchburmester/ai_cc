from analysis.DatabaseHandler import DatabaseHandler
from analysis.DataGenerator import DataGenerator
from analysis.simple_stats import generate_ngrams, remove_stopwords, remove_grams_with_stopwords
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
bigrams_cleaned_before = generate_ngrams(remove_stopwords(data['paper_abstract']), 2)
bigrams = generate_ngrams(data['paper_abstract'], 2)
bigrams_clean = remove_grams_with_stopwords(bigrams)
trigrams_cleaned_before = generate_ngrams(remove_stopwords(data['paper_abstract']), 3)
trigrams = generate_ngrams(data['paper_abstract'], 3)
trigrams_clean = remove_grams_with_stopwords(trigrams)
fourgrams_cleaned_before = generate_ngrams(remove_stopwords(data['paper_abstract']), 4)
fourgrams = generate_ngrams(data['paper_abstract'], 4)
fourgrams_clean = remove_grams_with_stopwords(fourgrams)

bigrams_freq = collections.Counter(bigrams_cleaned_before)
trigrams_freq = collections.Counter(trigrams_cleaned_before)
fourgrams_freq = collections.Counter(fourgrams_cleaned_before)

print('Bigrams:')
print(bigrams_freq.most_common(10))
print('Trigrams:')
print(trigrams_freq.most_common(10))
print('Fourgrams:')
print(fourgrams_freq.most_common(10))
