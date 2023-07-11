import pandas as pd
import numpy as np
import pickle
import os
from bertopic import BERTopic
import time
from functools import reduce
from sklearn.feature_extraction.text import CountVectorizer

start_time = time.time()

def main():

    csv = "results/en.ai_cc_2018_2023.csv"
    model = BERTopic.load(csv+".model")
    df = pd.read_csv(csv)
    docs = df['paper_abstract'].values.tolist()
    topics = df['topics'].values.tolist()
    probs = np.load(csv+".probabilities.npy")
         
    tf = model.get_topic_freq()
    print(f'\% of assigned documents: {tf.query("Topic != -1").Count.sum() / tf.Count.sum()}')
    print(f'# distinct non-outlier topics: {tf["Topic"].nunique()-1}')
    
    model.reduce_topics(docs, nr_topics = 'auto' )
         
    tf = model.get_topic_freq()
    print(f' \% of assigned documents:{tf.query("Topic != -1").Count.sum() / tf.Count.sum()}')
        
    red_topics = model.topics_
    df["reduced_topics"] = red_topics
    
    print(f'\% of assigned documents: {len(df.query("topics != -1")) / len(df)}')
    print(f'# distinct non-outlier reduced topics: {df["reduced_topics"].nunique()-1}')
    
    new_prob = model.probabilities_
    np.save(csv+".new.probabilities",new_prob)
    model.save(csv+".new.model")
        
    threshold = 0.2
    new_topics = model.reduce_outliers(docs, red_topics, strategy = "probabilities",probabilities= new_prob, threshold = threshold)
    df['p_topics'] = new_topics
    not_outliers = [t for t in new_topics if t != -1]
    print(f"outliers assigned with topic using probabilities: {100*len(not_outliers)/len(new_topics)}")
    
    dist_new_topics = model.reduce_outliers(docs, red_topics, strategy="distributions", threshold = threshold)
    df['d_topics'] = dist_new_topics
    not_outliers = [t for t in dist_new_topics if t != -1]
    print(f"outliers assigned with topic using distribution: {100*len(not_outliers)/len(dist_new_topics)}")
    
    ctfidf_new_topics = model.reduce_outliers(docs,red_topics, strategy="c-tf-idf", threshold=threshold)
    df["c_topics"] = ctfidf_new_topics
    not_outliers = [t for t in ctfidf_new_topics if t != -1]
    print(f"outliers assigned with topic using c-tf-idf: {100*len(not_outliers)/len(ctfidf_new_topics)}")
    
    emb_new_topics = model.reduce_outliers(docs,red_topics, strategy="embeddings", threshold=threshold)
    df["e_topics"]  = emb_new_topics
    not_outliers = [t for t in emb_new_topics if t != -1]
    print(f"outliers assigned with topic using embeddings: {100*len(not_outliers)/len(emb_new_topics)}")
        
    print(f'topics differing between probability and c-tf-idf assignment: {len(df.query("topics == -1")[["p_topics","d_topics","c_topics"]].query("p_topics != d_topics"))/len(df.query("reduced_topics == -1"))*100}')
    print(f'topics differing between probability and c-tf-idf assignment: {len(df.query("topics == -1")[["p_topics","d_topics","c_topics"]].query("p_topics != c_topics"))/len(df.query("reduced_topics == -1"))*100}')
    print(f'topics differing between probability and embedding assignment: {100*len(df.query("topics == -1")[["p_topics","d_topics","c_topics", "e_topics"]].query("p_topics != e_topics"))/len(df.query("reduced_topics == -1"))}')
    print(f'topics differing between c-tf-idf and embedding assignment: {100*len(df.query("topics == -1")[["p_topics","d_topics","c_topics", "e_topics"]].query("c_topics != e_topics"))/len(df.query("reduced_topics == -1"))}')    
    print(f'topics differing between distribution and embedding assignment: {100*len(df.query("topics == -1")[["p_topics","d_topics","c_topics", "e_topics"]].query("d_topics != e_topics"))/len(df.query("reduced_topics == -1"))}')
        
        

    df.to_csv(csv)
    representative_dict = model.get_representative_docs()
    with open(csv+ ".representative_docs.pkl", "wb") as f:
        pickle.dump(representative_dict, f)
    print("saved pickle and documents", flush=True)
    barchart = model.visualize_barchart(n_words=10, top_n_topics=30, height=500, width=500)
    barchart.write_html(csv+".new_barchart.html")
    ur_stop_words = pd.read_csv("datasets/stopwords_snowball_plus.csv")
        
    lng_mask = ur_stop_words["lng"].str.contains('en')
    stop_words = (
                ur_stop_words[lng_mask]
                .drop_duplicates(subset=["word"])["word"]
                .dropna()
                .values.tolist()
            )
        
    stop_words = [str(doc) for doc in stop_words if len(str(doc)) > 0]
    vectorizer_model = CountVectorizer(
            ngram_range = (1,3), stop_words=stop_words )
    model.update_topics(docs, topics=ctfidf_new_topics, n_gram_range = (1,3), vectorizer_model = vectorizer_model)
    representative_dict = model.get_representative_docs()
    with open(csv+ ".updated_representative_docs.pkl", "wb") as f:
        pickle.dump(representative_dict, f)
    print("saved updated pickle and documents", flush=True)
        
    topic_labels = model.generate_topic_labels(nr_words=10, separator=", ")
    with open(csv+".topic_labels.pkl","wb") as f:
        pickle.dump(topic_labels, f)
    print("saved topic labels", flush=True)
        

    barchart = model.visualize_barchart(n_words=10, top_n_topics=30, height=500, width=500)
    barchart.write_html(csv+".new_barchart.html")

if __name__ == "__main__":
    main()

def secondsToStr(t):
    return "%d:%02d:%02d.%03d" % \
        reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],
            [(t*1000,),1000,60,60])

print("--- Elapsed time: %s ---" % (secondsToStr(time.time() - start_time)))
