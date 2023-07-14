import spacy
import pandas as pd

# Load the English language model for spaCy
nlp = spacy.load('en_core_web_sm')

# Load the data
df = pd.read_csv('files/ai_cc_sust_1407.csv')

# Combine the title and abstract columns
df['text'] = df['paper_title'] + ' ' + df['paper_abstract']

# Put the text in lower case
df['text'] = df['text'].str.lower()

# Apply the spaCy model to the text
df['parsed'] = df['text'].apply(nlp)

# Print the parse tree for the first document
for token in df['parsed'][0]:
    print(token.text, token.dep_, token.head.text, token.head.pos_,
            [child for child in token.children])
    

def extract_svo(doc):
    subjs = [tok for tok in doc if (tok.dep_ == 'nsubj')]
    objs = [tok for tok in doc if (tok.dep_ in {'dobj', 'pobj'})]
    verbs = [tok for tok in doc if (tok.pos_ == 'VERB')]
    return subjs, verbs, objs

def filter_svo(subjs, verbs, objs):
    ai_terms = ['ai', 'artificial intelligence', ...]
    cc_terms = ['climate change', ...]
    filtered_subjs = [s for s in subjs if s.text.lower() in ai_terms]
    filtered_objs = [o for o in objs if o.text.lower() in cc_terms]
    return filtered_subjs, verbs, filtered_objs

