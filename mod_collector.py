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
from calc_gaps_slopes import neg_gap_calc 
from insert import insert_neg_gap

# load()

warnings.filterwarnings("ignore")
# TODO: process command line args if using graph_from_file()
# Read graph and convert to dataframe
start_time = time.time()
graph_read = read(sys.argv[1])
performance_data_df = pd.read_csv(sys.argv[2])
#print(performance_data_df.head())
read_comparison_values = read_comparison_values(graph_read)
comparison_values_df = to_dataframe(read_comparison_values)
comparison_values_df = comparison_values_df.reset_index()
#comparison_values_df.to_csv("comparison_values.csv")
read_goal_comparator = read_goal_comparator(graph_read)
read_social_comparator = read_social_comparator(graph_read)
neg_perf_trend =neg_perf_trend(graph_read)
neg_perf_trend_df = to_dataframe(neg_perf_trend)
pos_perf_trend =pos_perf_trend(graph_read)
pos_perf_trend_df = to_dataframe(pos_perf_trend)
read_gap_con =read_gap_con(graph_read)
pos_gap =pos_gap(graph_read)
pos_gap_df = to_dataframe(pos_gap)
neg_gap =neg_gap(graph_read)
neg_gap_df = to_dataframe(neg_gap)
neg_gap_sizes = neg_gap_calc(neg_gap_df,performance_data_df,comparison_values_df)
final_graph = insert_neg_gap(neg_gap_sizes,graph_read)
print(final_graph.serialize(format='json-ld', indent=4))
#with open(f'spek_mc.json', 'w') as output_file:
#    json.dump(final_graph, output_file)

