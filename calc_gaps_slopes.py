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
    comparison_values_df = get_comparison_values(neg_gap_df,comparison_values)
    goal_gap_size_df = calc_goal_comparator_gap(comparison_values_df,performance_data_df)
          
    return comparison_values_df
def get_comparison_values(neg_gap_df,comparison_values):
    neg_gap_df.reset_index(inplace=True)
    neg_gap_measures = neg_gap_df[['http://example.com/slowmo#RegardingMeasure{BNode}']]
    comparison_values_df = comparison_values[['index','http://example.com/slowmo#WithComparator{BNode}[0]','http://example.com/slowmo#WithComparator{BNode}[1]','http://example.com/slowmo#ComparisonValue{Literal}(xsd:double)']]
    
    GoalComparator = []
    SocialComparator =[]
    comparison_values_df['http://example.com/slowmo#WithComparator{BNode}[0]'] = comparison_values_df['http://example.com/slowmo#WithComparator{BNode}[0]'].fillna(0)
    comparison_values_df['http://example.com/slowmo#WithComparator{BNode}[1]'] = comparison_values_df['http://example.com/slowmo#WithComparator{BNode}[1]'].fillna("null")
    for rowIndex, row in comparison_values_df.iterrows():
        for columnIndex, value in row.items():
            
            if columnIndex == "http://example.com/slowmo#WithComparator{BNode}[0]":
                a = comparison_values_df.loc[comparison_values_df["index"] == value]
                if not a.empty:
                    a.reset_index(drop=True, inplace=True)
                    GoalComparator.append(a["http://example.com/slowmo#ComparisonValue{Literal}(xsd:double)"][0])
            if columnIndex == "http://example.com/slowmo#WithComparator{BNode}[1]":
                #print(comparison_values_df[columnIndex].values[rowIndex])
                if comparison_values_df[columnIndex].values[rowIndex] == "null" :
                    #print(comparison_values_df['index'].values[0])
                    SocialComparator.append(0)
                else:
                    c = comparison_values_df.loc[comparison_values_df["index"] == value]
                    if not c.empty:
                        c.reset_index(drop=True, inplace=True)
                        SocialComparator.append(c["http://example.com/slowmo#ComparisonValue{Literal}(xsd:double)"][0])  
    shape_comparison=comparison_values_df.shape
    lenb = shape_comparison[0]-len(GoalComparator)
    lenc = shape_comparison[0]-len(SocialComparator)
    GoalComparator.extend([0] * (lenb))
    SocialComparator.extend([0] * (lenc))
    #print(len(SocialComparator))
    comparison_values_df['GoalComparator']= GoalComparator
    comparison_values_df['SocialComparator']=SocialComparator 
    comparison_values_df.to_csv("comparison_values.csv")
    return comparison_values_df
def calc_goal_comparator_gap(comparison_values_df, performance_data):
    performance_data['Month'] = pd.to_datetime(performance_data['Month'])
    idx= performance_data.groupby(['Measure_Name'])['Month'].transform(max) == performance_data['Month']
    latest_measure_df = performance_data[idx]
    latest_measure_df['performance_data'] = latest_measure_df['Passed_Count'] / latest_measure_df['Denominator']
    comparison_values_df.rename(columns = {'index':'Measure_Name'}, inplace = True)
    final_df=pd.merge(comparison_values_df, latest_measure_df, on='Measure_Name', how='outer')
    lenb= len(latest_measure_df[['Passed_Count']])
    final_df1 = final_df[0:(lenb-1)]
    final_df1['performance_data'] = final_df1['performance_data'].fillna(0)
    #final_df['GoalComparator'].astype(float)
    final_df1['GoalComparator'] = pd.to_numeric(final_df1['GoalComparator'],errors='coerce')
    final_df1['SocialComparator'] = pd.to_numeric(final_df1['SocialComparator'],errors='coerce')
    final_df1['performance_data'] = pd.to_numeric(final_df1['performance_data'],errors='coerce')
    final_df1['goal_comparator_size'] = final_df1['GoalComparator']- final_df1['performance_data']
    final_df1['social_comparator_size'] = final_df1['SocialComparator']- final_df1['performance_data']
    #final_df1['goal_comparator_size'] = final_df1['performance_data'].fillna(0)
    #print(lenb)
    final_df1.to_csv("final_df.csv")
    #print(latest_measure_df.head())
    return comparison_values_df

