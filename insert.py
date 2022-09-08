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

def insert_gap(gap_df,graph_read):
    goal_comparator_df = gap_df[['goal_comparator_node','goal_comparator_size']]
    social_comparator_df = gap_df[['social_comparator_node','social_comparator_size']]
    for index ,row in gap_df.iterrows():
        node = row['goal_comparator_node']
        b_node = BNode(node)
        p = (URIRef("http://example.com/slowmo#PerformanceGapSize"))
        o = Literal(row['goal_comparator_size'])
        graph_read.add((b_node, p, o,))
        node1 = row['social_comparator_node']
        b_node1 = BNode(node1)
        p = (URIRef("http://example.com/slowmo#PerformanceGapSize"))
        o = Literal(row['social_comparator_size'])
        graph_read.add((b_node1, p, o,))
        
    #print(neg_gap_df.head())

   
    #b= "m1070"
    #a=BNode(b)
 
    #s = "m1069"
    #p =(URIRef("http://example.com/slowmo#PerformanceGapSize"))
    #o = Literal("25")
    #graph_read.add((a, p, o,))

    
    return graph_read

def insert_slope(gap_df,graph_read):
    gap_df = gap_df.drop_duplicates()
    #goal_comparator_df = gap_df[['goal_comparator_node','goal_comparator_size']]
    #social_comparator_df = gap_df[['social_comparator_node','social_comparator_size']]
    for index ,row in gap_df.iterrows():
        node = row['goal_comparator_node']
        b_node = BNode(node)
        p = (URIRef("http://example.com/slowmo#PerformanceTrendSlope"))
        o = Literal(row['0'])
        graph_read.add((b_node, p, o,))
        node1 = row['social_comparator_node']
        b_node1 = BNode(node1)
        p = (URIRef("http://example.com/slowmo#PerformanceGapSize"))
        o = Literal(row['0'])
        graph_read.add((b_node1, p, o,))
        
    #print(neg_gap_df.head())

   
    #b= "m1070"
    #a=BNode(b)
 
    #s = "m1069"
    #p =(URIRef("http://example.com/slowmo#PerformanceGapSize"))
    #o = Literal("25")
    #graph_read.add((a, p, o,))

    
    return graph_read

 