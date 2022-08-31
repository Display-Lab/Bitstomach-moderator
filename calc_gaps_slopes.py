import warnings
import time
import logging

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

warnings.filterwarnings("ignore")
def neg_gap_calc(neg_gap_df , performance_data_df, comparison_values):
    neg_gap_df.reset_index(inplace=True)
    neg_gap_measures = neg_gap_df[['http://example.com/slowmo#RegardingMeasure{BNode}']]
    comparison_values_df = comparison_values[['index','http://example.com/slowmo#WithComparator{BNode}[0]','http://example.com/slowmo#WithComparator{BNode}[1]','http://example.com/slowmo#ComparisonValue{Literal}(xsd:double)']]
    comparison_values_df.to_csv("comparison_values.csv")
    return neg_gap_measures