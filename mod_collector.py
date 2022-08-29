import json
import sys
import warnings
import time
import logging
import json
#from asyncore import read

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

from load import read ,read_goal_comparator,read_comparison_values,read_social_comparator,neg_perf_trend,read_gap_con,pos_perf_trend,pos_gap,neg_gap
#, read_perf_data ,read_gaps
#,write
#from .calc_gaps_slopes import calc_gap_sizes ,calc_slope_trends

# load()

warnings.filterwarnings("ignore")
# TODO: process command line args if using graph_from_file()
# Read graph and convert to dataframe
start_time = time.time()
graph_read = read(sys.argv[1])
read_comparison_values = read_comparison_values(graph_read)
read_goal_comparator = read_goal_comparator(graph_read)
read_social_comparator = read_social_comparator(graph_read)
neg_perf_trend =neg_perf_trend(graph_read)
pos_perf_trend =pos_perf_trend(graph_read)
read_gap_con =read_gap_con(graph_read)
pos_gap =pos_gap(graph_read)
pos_gap_df = to_dataframe(pos_gap)
pos_gap_df.to_csv("measures1.csv")
neg_gap =neg_gap(graph_read)
neg_gap_df = to_dataframe(neg_gap)
neg_gap_df.to_csv("measures2.csv")

