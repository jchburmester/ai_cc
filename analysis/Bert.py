import pandas as pd
import numpy as np

import umap
import os
import pickle

from bertopic import BERTopic
from math import sqrt
from sklearn.feature_extraction.text import CountVectorizer
from argparse import ArgumentParser
from argparse import ArgumentError
from collections import Counter