import warnings
import time
import logging
import csv

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef ,BNode
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD 
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

warnings.filterwarnings("ignore")

def insert_neg_gap(neg_gap_df,graph_read):
    a=BNode("m1069")
    #f = open('graph.txt', 'w')
    #(URIRef("http://www.w3.org/exmple")
    s = "m1069"
    p =(URIRef("http://example.com/slowmo#PerformanceGapSize"))
    o = Literal("25")
    graph_read.add((a, p, o,))

    
    return graph_read