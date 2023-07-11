from bertopic import BERTopic

import pandas as pd
import numpy as np
import umap
from sklearn.feature_extraction.text import CountVectorizer
from math import sqrt
import os
import pickle
from argparse import ArgumentParser
from argparse import ArgumentError
from collections import Counter


def main():
    parser = ArgumentParser(
        description="Script to automate Topic Modeling with BERTopic. The script will produce a model, augment the data, and some charts to assess the quality of the model such as barchart (-b), topic over time (-t), topic per class (-k); tested for barchart plots only. Information on hyperparameters can be found in the section below, or in the BERTopic official documentation: https://maartengr.github.io/BERTopic/getting_started/parameter%20tuning/parametertuning.html",
    )
    parser.add_argument(
        "CSVs",
        nargs="+",
        help="CSVs of documents to be modelled, possessing a column of documents 'text' (--text_column to change it), and a column of document ids 'id' (--id_column to change it); more than one file can be passed as argument. If datasets are in different languages, the --language (-l) option can be used to input a list of languages of the same length of the number of CSVs.",
    )
    parser.add_argument(
        "-D",
        "--results_directory",
        dest="res_dir",
        help="Folder where results are stored; it is created if id does not exist ; default is 'results'.",
        action="store",
        type=str,
        default="results",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        help="Prints actions while program is running.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-l",
        "--language",
        dest="lng",
        help="Acronym of the languages used; inputting more languages will be used to match languages and the various CSVs to be modeled; if more languages are given than the number of CSVs, then stopwords of all given languages are omitted, e.g.: when two csv are given, imputting -l 'it' 'de' 'en' 'fr' will omit english and french from italian and german documents: in this case the language is considered to be the first element of the list; the choices compatible with the given snowball stopwords are 'en', 'de', 'fr', 'it'; 'en' will set the embedding to the 'english' one, while all other choices will set it to the 'multilingual' one; consult BERTopic documentation for available choices; default is ['en'].",
        action="store",
        default=["en"],
        type=str,
        nargs="*",
    )
    parser.add_argument(
        "--more_stopwords",
        dest="more_stopwords",
        help="Stopwords which are not in the stopword dataset. useful for context specific topic modeling",
        action="store",
        type=str,
        nargs="*",
        default=None,
    )
    parser.add_argument(
        "--text_column",
        dest="text_col",
        help="name of the csv column with the documents to be modelled; default = 'text'.",
        action="store",
        type=str,
        default="text",
    )
    parser.add_argument(
        "--id_column",
        dest="id_col",
        help="name of the csv column with the id of the documents to be modelled; default = 'id'.",
        action="store",
        type=str,
        default="id",
    )

    BERTopic_opt = parser.add_argument_group("BERTopic Options")
    BERTopic_opt.add_argument(
        "-e",
        "--embedding",
        dest="embedding_model",
        action="store",
        help="Word Embedding employed; if 'auto', the model is 'all-mpnet-base-v2' for english, and 'paraphrase-multilingual-mpnet-base-v2' for all other languages; for custom choices consult https://www.sbert.net/docs/pretrained_models.html; default is 'auto'.",
        type=str,
        default="auto",
    )
    BERTopic_opt.add_argument(
        "--V",
        dest="BERTopic_verbose",
        help="Sets BERTopic verbose option to True; warmly discuraged if writing to file log e.g. with nohup, as it creates textfiles of several MB, even GB, as progress bar is continuously written to file.",
        action="store_false",
        default=False,
    )
    BERTopic_opt.add_argument(
        "-N",
        "--nr_topics",
        dest="nr_topics",
        help="Number of output topics, used for topic reduction; if 'auto' or an integer is given, it will (automatically) aggregate similar documents to produce a bigger cluster; it may be more sensible to call topic_model.reduce_topics(docs, nr_topics=<N>)after the model has been created, as topic reduction is non-reversible and it may produce Frankenstein clusters with very long descriptions, making analysis more difficult; default is None.",
        action="store",
        default=None,
    )
    BERTopic_opt.add_argument(
        "-s",
        "--min_topic_size",
        dest="min_topic_size",
        help="Minimum topic size; when dealing with >10^7 documents it may be sensible to set it to 100 or even 500; set it too small and many micro-clusters will appear, but set it too high and no cluster will be generated; if 'auto' or 'sqrt' it will approximate a 'good number' for very large datasets (no guarantees); default is 10.",
        action="store",
        default=10,
        type=int,
    )

    UMAP_opt = parser.add_argument_group("UMAP Options")
    UMAP_opt.add_argument(
        "-n",
        "--n_neighbours",
        dest="n_neighbours",
        action="store",
        help="Number of neighbours considered by UMAP; increasing/decreasing the value leads to bigger/smaller clusters and a more global/local view. defaults to 15.",
        type=int,
        default=15,
    )
    UMAP_opt.add_argument(
        "-c",
        "--n_components",
        dest="n_components",
        action="store",
        help="Number of components UMAP reduces dimensionality to; lower values lead to low quality embeddings, increasing it will greatly affect UMAP complexity, if a metric appropriate for high dimension is not chosen; defaults to 5.",
        type=int,
        default=5,
    )
    UMAP_opt.add_argument(
        "-m",
        "--metric",
        dest="metric",
        action="store",
        help="Metric employed by UMAP to compute distance; other metrics (listed in https://umap-learn.readthedocs.io/en/latest/parameters.html#metric) can be more sensible (see --n_components), but the default is 'cosine'.",
        type=str,
        default="cosine",
    )
    UMAP_opt.add_argument(
        "-p",
        "--calculate_probabilities",
        dest="p_umap",
        action="store_true",
        help="Calculates probabilities for slower but more effective UMAP; suggested for accurate results or small dataset.",
        default="False",
    )
    UMAP_opt.add_argument(
        "--low_memory",
        dest="low_memory",
        action="store_true",
        default=False,
        help="Slows computation but makes it possible to produce results in machines with scarse memory; disabled by default.",
    )

    vectorizer_opt = parser.add_argument_group("vectorizer options")
    vectorizer_opt.add_argument(
        "-g",
        "--n_gram",
        dest="n_gram",
        action="store",
        help="Two arguments to indicate the minimum and maximum n_gram considered; it explodes quickly, but may be useful for languages with many ; if you want to consider 'New York', or phrasal verbs as a single word, you should set it to 1 2; default is 1 1.",
        default=(1, 1),
        nargs=2,
        type=int,
    )
    vectorizer_opt.add_argument(
        "-d",
        "--min_document_frequency",
        dest="min_df",
        action="store",
        help="Set the minimum document frequency for a word to be kept (setting it to 2 will trim every word included in just 1 document); used to trim outliers and keep topic representation smaller; default is 3, but can be set higher outside a streaming setting.",
        default=3,
        type=int,
    )
    vectorizer_opt.add_argument(
        "-S",
        "--stopwords",
        dest="stopwords_dir",
        help="Directory of stopword lexicon stored in 'datasets' directory; it is technically possible to use none, but it is recommended not to do so; default : 'stopwords_snowball_plus.csv'.",
        action="store",
        type=str,
        default="stopwords_snowball_plus.csv",
    )

    plot_opt = parser.add_argument_group("plot options")
    plot_opt.add_argument(
        "-b",
        "--plot_barchart",
        dest="want_barchart",
        action="store_true",
        help="Plot barchart of 20 most numerous topics.",
        default=False,
    )
    plot_opt.add_argument(
        "-t",
        "--plot_over_time",
        dest="want_time_plot",
        action="store_true",
        help="Plot topics over time using column name 'date' as timestamp.",
        default=False,
    )
    plot_opt.add_argument(
        "--timestamp_column",
        dest="timestamp_col",
        help="Name of the csv column with the timestamp for the 'Topic over time' plot; default is 'date'.",
        action="store",
        type=str,
        default="date",
    )
    plot_opt.add_argument(
        "-k",
        "--plot_per_class",
        dest="want_class_plot",
        action="store_true",
        help="Plot topics per class using column name 'class' as class column name.",
        default=False,
    )
    plot_opt.add_argument(
        "--classes_column",
        dest="class_col",
        help="Name of the csv column with the timestamp for the 'Topic over time' plot; default is 'class'.",
        action="store",
        type=str,
        default="class",
    )
    plot_opt.add_argument(
        "--datetime_format",
        dest="datetime_format",
        type=str,
        action="store",
        help="Format of the timestamp used by python. If 'None', it will be automatically infered; E.g.: for 2023-04-26T15:20:26Z the format is '%%Y-%%m-%%dT%%H:%%M:%%SZ'; for more info, check https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes; default is '%%Y-%%m-%%d'.",
        default=None,
    )
    debug_opt = parser.add_argument_group("DEBUG")
    debug_opt.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="for debugging purposes",
    )

    args = parser.parse_args()

    args.stopwords_dir = os.path.join("datasets", args.stopwords_dir)

    if args.nr_topics is not None and args.nr_topics != "auto":
        try:
            args.nr_topics = int(args.nr_topics)
        except:
            raise (
                ArgumentError(
                    message="-N or --nr_topics invalid: remove the option or set it to a valid integer or to auto."
                )
            )

        if (
            len(args.lng) != 1
            and len(args.CSVs) != 1
            and len(args.lng) < len(args.CSVs)
        ):
            raise (
                ArgumentError(
                    message="--languages must be at least one, or more than the number elements in the csv list."
                )
            )

    # for BERTopic model

    for i, csv in enumerate(args.CSVs):
        ur_stop_words = pd.read_csv(args.stopwords_dir)

        ur_docs = pd.read_csv(csv)
        if args.debug:
            ur_docs = ur_docs.sample(n=10000)
        lng = args.lng[0]
        if len(args.lng) > 1:
            lng = args.lng[i]

        # calculate word count and text length
        # ur_docs['words'] = ur_docs['text'].str.count('\s+')+1
        # ur_docs['text_len'] = ur_docs['text'].apply(len)
        # print shape and other metadata
        # print("Before short sentence removal: ",ur_docs.shape)
        # ur_docs = ur_docs.query('words > 2')
        # print("After short sentence removal: ",ur_docs.shape)
        # print(ur_docs.columns)
        mask = ur_docs[args.text_col].str.len() >= 1
        ur_docs = ur_docs.loc[mask]
        # cast documents to 'str'
        if args.text_col not in ur_docs.columns:
            raise (
                ArgumentError(
                    message=f"Missing document column '{args.text_col}' in {csv}.\nSet --text_column to the name of the column with the documents to be modelled, or change the corresponding csv column name to '{args.text_col}'."
                )
            )
        if args.id_col not in ur_docs.columns:
            raise (
                ArgumentError(
                    message=f"Missing column '{args.id_col}' in {csv}.\nSet --id_column to the name of the column with the documents IDs, or change the corresponding csv column name to '{args.id_col}'."
                )
            )
        if args.want_time_plot and args.timestamp_col not in ur_docs.columns:
            raise (
                ArgumentError(
                    message=f"Missing column '{args.timestamp_col}' in {csv}.\nSet --timestamp_column to the name of the column with the document timestamps, or change the corresponding csv column name to '{args.timestamp_col}' (or remove the -t option)."
                )
            )
        if args.want_class_plot and args.class_col not in ur_docs.columns:
            raise (
                ArgumentError(
                    message=f"Missing column '{args.class_col}' in {csv}.\nSet --classes_column to the name of the column with the document classes, or change the corresponding csv column name to '{args.class_col}' (or remove the -k option)."
                )
            )
        ur_docs[args.text_col] = ur_docs[args.text_col].astype("str")
        ur_docs[args.id_col] = ur_docs[args.id_col].astype("str")

        # dictionaries to select languages:

        if args.verbose:
            print(
                f"Creating instance of custom UMAP model:\n\tn_neighbours = {args.n_neighbours}\n\tn_components = {args.n_components}\n\tmetric = {args.metric}",
                flush=True,
            )

        umap_model = umap.UMAP(
            n_neighbors=args.n_neighbours,
            n_components=args.n_components,
            metric=args.metric,
            low_memory=args.low_memory,
            # n_jobs = 1, # increase to use parallel computing. It breaks on Jupyter NB on my MAC
            # random_state = 42, # comment out for performance, at expense of reproducibility
        )

        os.makedirs(args.res_dir, exist_ok=True)

        if args.verbose:
            print("Preparing to extract ", flush=True)

        # extract stopwords
        # by masking the language
        if len(args.CSVs) < len(args.lng) and len(args.lng) > 1:
            lng_mask = ur_stop_words["lng"].str.contains("|".join(args.lng))
        else:
            lng_mask = ur_stop_words["lng"].str.contains(lng)

        stop_words = (
            ur_stop_words[lng_mask]
            .drop_duplicates(subset=["word"])["word"]
            .dropna()
            .values.tolist()
        )
        if args.more_stopwords is not None:
            if type(args.more_stopwords) == type(list()):
                for w in args.more_stopwords:
                    stop_words.append(w)
            elif type(args.more_stopwords) == type("asdf"):
                stop_words.append(args.more_stopwords)

        stop_words = [str(doc) for doc in stop_words if len(str(doc)) > 0]

        # get embedding model
        if args.embedding_model == "auto":
            embedding_model = "all-mpnet-base-v2"
            if not lng == "en":
                embedding_model = "paraphrase-multilingual-mpnet-base-v2"
        else:
            embedding_model = args.embedding_model

        counter = 0
        for e in stop_words:
            if e == "nan":
                counter += 1
        if args.debug:
            print(
                f"There were {counter} nan values among the provided stopwords.",
                flush=True,
            )

        # topic_model.get_representative_docs()

        stop_words = stop_words[: (len(stop_words) - counter)]

        docs = ur_docs[args.text_col].values.tolist()

        if args.min_topic_size == "auto":
            doc_len = len(docs)
            doc_pow = len(str(doc_len)) - 1
            doc_rounded = round(doc_len / 10**doc_pow)
            min_topic_size = round(doc_rounded * 10**doc_pow / 100000)
            min_topic_size = max(10, min_topic_size)
        elif args.min_topic_size == "sqrt":
            min_topic_size = int(sqrt(len(docs) // 100))
            min_topic_size = max(10, min_topic_size)
        else:
            min_topic_size = args.min_topic_size
        # if min_df < 18:
        #    min_df *= 2
        print(f"min topic size set to {args.min_topic_size}", flush=True)
        if args.verbose:
            print(
                f"Creating instance of custom Vectorizer model:\n\tngram range = {args.n_gram}\n\tminimum document frequency = {args.min_df}\n\tstopwords from = {args.stopwords_dir} ",
                flush=True,
            )
        # Create custom Vectorizer used to take stopwords away and use n-grams
        try:
            if args.verbose:
                print(
                    f"Creating instance of custom Vectorizer model:\n\tngram range = {args.n_gram}\n\tminimum document frequency = {args.min_df}\n\tstopwords from = {args.stopwords_dir} ",
                    flush=True,
                )
            vectorizer_model = CountVectorizer(
                ngram_range=args.n_gram, stop_words=stop_words, min_df=args.min_df
            )  # min document frequency: automatically trims outliers, improves performance.
            if args.verbose:
                print(
                    f"Creating instance of custom BERTopic object:\n\tembedding model = {embedding_model}\n\tlanguage = {lng}\n\tcalculate probabilities? {args.p_umap}\n\tnumber of topics = {args.nr_topics}\n\tminimum topic size = {args.min_topic_size} : {min_topic_size}",
                    flush=True,
                )

            topic_model = BERTopic(
                embedding_model=embedding_model,
                umap_model=umap_model,
                vectorizer_model=vectorizer_model,
                language="english" if lng == "en" else "multilingual",
                calculate_probabilities=args.p_umap,
                verbose=args.BERTopic_verbose,
                nr_topics=args.nr_topics,
                min_topic_size=min_topic_size,
            )
            if args.verbose:
                print("Starting toppic modelling.", flush=True)
            if args.p_umap:
                topics, probs = topic_model.fit_transform(docs)
            else:
                topics = topic_model.fit_transform(docs)

            ur_docs["topics"] = topics

            # save stuff

            if args.verbose:
                print("Saving new dataset", flush=True)
            ur_docs.to_csv(os.path.join(args.res_dir, ".".join((lng, csv))))
            if args.p_umap:
                np.save(
                    os.path.join(args.res_dir, ".".join((lng, csv, "probabilities"))),
                    probs,
                )
            topic_model.save(
                os.path.join(args.res_dir, ".".join((lng, csv + ".model")))
            )

            representative_dict = topic_model.get_representative_docs()
            with open(
                os.path.join(
                    args.res_dir, ".".join((lng, csv, "representative_docs.pkl"))
                ),
                "wb",
            ) as f:
                pickle.dump(representative_dict, f)

            print(
                f"\nmodel for {csv} created succesfully.\nthere are {len(set(topics))-1} topics distinct from -1\nproportion of documents with a topic different from -1 is {round( ((len(topics) - Counter(topics)[-1])/len(topics)*100),2) }%\n"
            )
            # with open('saved_dictionary.pkl', 'rb') as f:
            #    loaded_dict = pickle.load(f)

            if args.want_barchart:
                barchart = topic_model.visualize_barchart(
                    n_words=20, top_n_topics=30, height=500, width=500
                )
                barchart.write_image(
                    os.path.join(args.res_dir, ".".join((lng, csv, "barchart.png"))),
                    width=1500,
                    height=700,
                )
                barchart.write_image(
                    os.path.join(args.res_dir, ".".join((lng, csv, "barchart.pdf"))),
                    width=1500,
                    height=700,
                )
                barchart.write_html(
                    os.path.join(args.res_dir, ".".join((lng, csv, "barchart.html")))
                )
                """
                barchart.write_json(
                    os.path.join(args.res_dir, ".".join((lng, csv, "barchart.json")))
                )
                """
            if args.want_time_plot:
                timestamps = ur_docs[args.timestamp_col].values.tolist()
                if args.datetime_format is None:
                    args.datetime_format = "%Y-%m-%d"
                topics_over_time = topic_model.topics_over_time(
                    docs,
                    topics=topics,
                    timestamps=timestamps,
                    datetime_format=args.datetime_format,
                )

                topics_over_time.write_image(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_over_time.png"))
                    ),
                    width=1500,
                    height=700,
                )
                topics_over_time.write_image(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_over_time.pdf"))
                    ),
                    width=1500,
                    height=700,
                )
                topics_over_time.write_html(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_over_time.html"))
                    )
                )
                """
                topics_over_time.write_json(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_over_time.json"))
                    )
                )
                """
            if args.want_class_plot:
                classes = ur_docs[args.class_col].values.tolist()
                topics_per_class = topic_model.topics_per_class(
                    docs,
                    classes=classes,
                    datetime_format=args.datetime_format,
                    topics=topics,
                )
                topics_per_class.write_image(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_per_class.png"))
                    ),
                    width=1500,
                    height=700,
                )
                topics_per_class.write_image(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_per_class.pdf"))
                    ),
                    width=1500,
                    height=700,
                )
                topics_per_class.write_html(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_per_class.html"))
                    )
                )
                """
                topics_per_class.write_json(
                    os.path.join(
                        args.res_dir, ".".join((lng, csv, "topics_per_class.json"))
                    )
                )
                """
            print(f"\plots for {csv} created succesfully\n")
        except Exception as err:
            errstr = f"error occurred during modeling {lng+'.'+csv},\nerr = {err},\ntype = {type(err)}\n,args = {err.args}."
            print(errstr, flush=True)
    print("DONE")


if __name__ == "__main__":
    main()
