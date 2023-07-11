import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from umap import UMAP
import os
from functools import reduce

def main():
    my_dir = "hierarchic"
    os.makedirs(my_dir, exist_ok=True)
    csv = "results/en.ai_cc_2018_2023.csv"#en.ai_cc_2018_2023.csv.new.model
    model = BERTopic.load(csv+".new.model")
    model.verbose = False
    df = pd.read_csv(csv)
    docs = df['paper_abstract'].values.tolist()
    topics = model.topics_#df['c_topics'].values.tolist()
    #probs = np.load(csv+".probabilities.npy")
    print('computing hierarchical topics', flush=True)
    hierarchical_topics = model.hierarchical_topics(docs)
    
    print('computing reduced embeddings', flush=True)
    sentence_model = SentenceTransformer('all-mpnet-base-v2')
    embeddings = sentence_model.encode(docs, show_progress_bar=False)
    reduced_embeddings = UMAP(n_neighbors=10, n_components=2, min_dist=0.0, metric='cosine').fit_transform(embeddings)

    #model.visualize_hierarchy(hierarchical_topics=hierarchical_topics)
    print('saving figure')
    fig = model.visualize_hierarchical_documents(docs, hierarchical_topics, reduced_embeddings=reduced_embeddings)
    fig.write_html(my_dir+"/hierarchical_docs.html")

if __name__ == "__main__":
    main()
def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

print("--- Elapsed time: %s ---" % (secondsToStr(time.time() - start_time)))
