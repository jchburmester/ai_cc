from nltk.util import ngrams
from nltk.corpus import stopwords


# N-grams
def generate_ngrams(data, n):
    data = data.to_string(index=False)
    words = data.split()

    ngrams_list = ngrams(words, n)
    
    return list(ngrams_list)

# Remove stopwords
def remove_stopwords(data):
    data = data.to_string(index=False)
    words = data.split()
    
    stop_words = set(stopwords.words('english'))
    filtered_words = [word for word in words if word not in stop_words]
    
    return filtered_words

# Remove n-grams with stopwords
def remove_grams_with_stopwords(data):
    english_stopwords = set(stopwords.words('english'))
    
    filtered_data = [gram for gram in data if not any((word.lower() in english_stopwords) for word in gram)]
    
    return filtered_data