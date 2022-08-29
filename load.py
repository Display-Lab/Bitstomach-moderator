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


def read(file):
    start_time = time.time()
    g = Graph()
    g.parse(file)
    logging.critical(" reading graph--- %s seconds ---" % (time.time() - start_time)) 
    return g
def read_comparison_values(graph_read):
    start_time = time.time()
    qres = graph_read.query(
        """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutMeasure ?measure .
        ?measure slowmo:WithComparator ?o3 .
        ?o3 slowmo:ComparisonValue ?o4 .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutMeasure ?measure .
        ?measure slowmo:WithComparator ?o3 .
        ?o3 slowmo:ComparisonValue ?o4 .
        
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph

def read_goal_comparator(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000094 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000094 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph
def read_social_comparator(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000095 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000095 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph

def neg_perf_trend(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000100 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000100 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph
def read_gap_con(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000106 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000106 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph

def pos_perf_trend(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000099 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000099 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph
def pos_gap(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000104 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000104 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph

def neg_gap(graph_read):
    start_time = time.time()
    qres = graph_read.query(
    """
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX slowmo: <http://example.com/slowmo#>
    construct {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000105 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
        
    } 
    WHERE {
        ?candidate slowmo:IsAboutPerformer ?performer .
        ?performer obo:RO_0000091 ?o2 .
        ?o2 a obo:psdo_0000105 .
        ?o2 slowmo:RegardingComparator ?comparator .
        ?o2 slowmo:RegardingMeasure ?measure .
    }
    """
    )
    logging.critical(" querying contenders graph--- %s seconds ---" % (time.time() - start_time)) 
    return qres.graph


