from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
import pandas as pd


model_path="paraphrase-distilroberta-base-v1"
model = SentenceTransformer(model_path)

# Define csv file path
csv_file_path = 'C:/Users/jch/ai_cc/files/ai_cc_2018_2023.csv'

# Open csv file
df = pd.read_csv(csv_file_path)

# Get abstracts
abstracts = df['paper_abstract'].tolist()

# Get embeddings
embeddings = model.encode(abstracts)
embeddings.shape

K = 5
kmeans = KMeans(n_clusters=5,random_state=0).fit(embeddings)
cls_dist=pd.Series(kmeans.labels_).value_counts()