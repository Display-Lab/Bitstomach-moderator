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

