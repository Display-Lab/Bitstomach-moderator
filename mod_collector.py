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

from load import read ,read_gaps_trends,read_comparison_values
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
read_gaps_trends = read_gaps_trends(graph_read)
read_gaps_trends_df = to_dataframe(read_gaps_trends)
read_gaps_trends_df.to_csv("measures1.csv")

