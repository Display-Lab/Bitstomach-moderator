from tracemalloc import start
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
import numpy as np 

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
    
    #gap_df = gap_df.drop_duplicates()
    #goal_comparator_df = gap_df[['goal_comparator_node','goal_comparator_size']]
    #social_comparator_df = gap_df[['social_comparator_node','social_comparator_size']]
    for index ,row in gap_df.iterrows():
        node = row['Measure_Name']
        #print(node)
        b_node = BNode(node)
        p = (URIRef("http://example.com/slowmo#PerformanceTrendSlope"))
        o = Literal(row['performance_trend_slope'])
       
        graph_read.add((b_node, p, o,))
        
    #print(neg_gap_df.head())

   
    #b= "m1070"
    #a=BNode(b)
 
    #s = "m1069"
    #p =(URIRef("http://example.com/slowmo#PerformanceGapSize"))
    #o = Literal("25")
    #graph_read.add((a, p, o,))

    
    return graph_read

def insert_trend(slope_graph,monotonic_pred_df):
    
    
    for rowIndex, row in monotonic_pred_df.iterrows():  # iterate over rows
        node = "p1"
        b_node = BNode(node)
        p= Literal("http://purl.obolibrary.org/obo/RO_0000091")
        o = BNode()
        slope_graph.add((b_node,p,o))
        s=o
        p1 = RDF.type
        if row['trend']== "no trend":
            trend ="http://example.com/slowmo#NoTrend"
        elif row['trend']== "monotonic":
            trend ="http://example.com/slowmo#MonotonicTrend"
        elif row['trend']== "non-monotonic":
            trend ="http://example.com/slowmo#NonMonotonicTrend"
        node1 = Literal(trend)
        o1 = BNode(node1)
        slope_graph.add((s,p1,o1))
        s1 = o
        p2=Literal("http://example.com/slowmo#RegardingComparator")
        node1 = row["goal_comparator_node"] 
        o2=BNode(node1)
        slope_graph.add((s1,p2,o2))
        s2 = o
        p3=Literal("http://example.com/slowmo#RegardingMeasure")
        node1 =  row["Measure_Name"]
        o3=BNode(node1)
        slope_graph.add((s2,p3,o3))

    # slope_graph_df =  to_dataframe(slope_graph)
    # slope_graph_df.reset_index(inplace=True)
    # a = slope_graph_df.loc[slope_graph_df["index"] == "p1"]
    # a.reset_index(inplace = True)
    # a= a.T
    # a.rename(columns = {0:'index'}, inplace = True)
    # a=a.astype(str)
    # #a['index']=a['index'].astype('|S')
    # #a.to_csv("a.csv")
    # b=a.values.tolist()
    # #b.remove(np.nan)
    # #print(b)
    # subject_node=[]
    # #start_letter = "N"
    # #with_s = list(filter(lambda x: x.startswith(start_letter),b))
    # #print(with_s)
    # for x in range(len(b)):
    #     #x = *b[x].startswith("N")
    #     if(b[x][0].startswith("N")):
    #         subject_node.append(b[x][0])
    #     #print(b[x][0].startswith("N"))
    #     #print(type(*b[x]))
    #     #print(y)    
    #     #print(str(b[x]))
    #     #print(type(b[x]))
    #     #print (b[x])
    #     #if(b[x]=="nan"):
    #        # ab.append(b[x])
    # for x in range(len(subject_node)):
    #     #print(subject_node[x])
    #     node = subject_node[x]
    #     b_node = BNode(node)
    #     p = RDF.type
    #     o = Literal("http://example.com/slowmo#MonotonicTrend")
    #     #slope_graph.add((b_node,p,o))
    #     # s = o
    #     # p=Literal("http://example.com/slowmo#RegardingComparator")
    #     # node1 = "m1069"
    #     # o1=BNode(node1)
    #     # slope_graph.add((s,p,o1))
    # #a.to_csv("a.csv")



    # node = "N00e33dbd0047463d8da330a1ee72d275"
    # b_node = BNode(node)
    # p = RDF.type
    # o = Literal("http://example.com/slowmo#MonotonicTrend")
    # slope_graph.add((b_node,p,o))
    # s = o
    # p=Literal("http://example.com/slowmo#RegardingComparator")
    # node1 = "m1069"
    # o1=BNode(node1)
    # slope_graph.add((s,p,o1))
    
    # node= "p1"
    # b_node = BNode(node)
    # p=Literal("http://purl.obolibrary.org/obo/RO_0000091")
    # node1 = "m1069"
    # o= BNode(node1)
    # o1= Literal("http://example.com/slowmo#MonotonicTrend")
    # slope_graph.add((b_node,p,o))
    # slope_graph.add((o,RDF.type,o1))


    # node = "p1"
    # b_node = BNode(node)
    # p= Literal("http://purl.obolibrary.org/obo/RO_0000091")
    
    # o = BNode()
    # slope_graph.add((b_node,p,o))
    # s=o
    # p1 = RDF.type
    # node1 = Literal("http://example.com/slowmo#MonotonicTrend")
    # o1 = BNode(node1)
    # slope_graph.add((s,p1,o1))
    # s1 = o
    # p2=Literal("http://example.com/slowmo#RegardingComparator")
    # node1 = "m1069" 
    # o2=BNode(node1)
    # slope_graph.add((s1,p2,o2))
    # s2 = o
    # p3=Literal("http://example.com/slowmo#RegardingMeasure")
    # node1 =  "GA01"
    # o3=BNode(node1)
    # slope_graph.add((s2,p3,o3))

    return slope_graph


 