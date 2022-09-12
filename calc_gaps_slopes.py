from pickle import TRUE
import warnings
import time
import logging

import pandas as pd
import scipy
from scipy import stats
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import FOAF, RDF, RDFS, SKOS, XSD
from rdflib.serializer import Serializer
from rdfpandas.graph import to_dataframe
from SPARQLWrapper import XML, SPARQLWrapper

warnings.filterwarnings("ignore")
def gap_calc( performance_data_df, comparison_values):
    comparison_values_df = get_comparison_values(comparison_values)
    goal_gap_size_df = calc_goal_comparator_gap(comparison_values_df,performance_data_df)
    
    return goal_gap_size_df

def trend_calc(performance_data_df,comparison_values):
    #print(performance_data_df.head())
    #neg_goal_gap_size_df.to_csv("final_neg_gap.csv")      
    performance_data_df['Month'] = pd.to_datetime(performance_data_df['Month'])
    lenb= len( performance_data_df[['Measure_Name']].drop_duplicates())
    #print(lenb)
    idx= performance_data_df.groupby(['Measure_Name'])['Month'].nlargest(3) .reset_index()
    l=idx['level_1'].tolist()
    #performance_data_df =performance_data_df.reset_index()
    
    #df_1 = pd.DataFrame(idx).reset_index()
    #idx1 = df_1['Month'].reset_index(drop=True) == performance_data_df['Month'].reset_index(drop=True)
    #print(df_1)
    #print(type(df_1["level_1"]))
    


    latest_measure_df =  performance_data_df[performance_data_df.index.isin(l)]
    latest_measure_df['performance_data'] = latest_measure_df['Passed_Count'] / latest_measure_df['Denominator']
    latest_measure_df['performance_data']=latest_measure_df['performance_data'].fillna(0)
    latest_measure_df.to_csv("latest_measure.csv")
    out = latest_measure_df.groupby('Measure_Name').apply(theil_reg, xcol='Month', ycol='performance_data')
    df_1=out[0]
    df_1 = df_1.reset_index()
    df_1 = df_1.rename({"0":"performance_trend_slope"}, axis=1)
    slope_df = pd.merge( latest_measure_df,df_1 , on='Measure_Name', how='outer')
    comparison_values_df = get_comparison_values(comparison_values)
    comparison_values_df.rename(columns = {'index':'Measure_Name'}, inplace = True)
    slope_final_df =pd.merge( comparison_values_df,slope_df , on='Measure_Name', how='outer')
    slope_final_df = slope_final_df.reset_index(drop = True)
    #print(slope_final_df.head())
    slope_final_df=slope_final_df.drop_duplicates(subset=['Measure_Name'])
    slope_final_df = slope_final_df.rename({0: 'performance_trend_slope'}, axis=1)
    #lenb= len(slope_final_df[['Passed_Count']])
    #print(lenb)
    slope_final_df = slope_final_df[:(lenb-1)]
    slope_final_df= slope_final_df[['Measure_Name','performance_data','performance_trend_slope']]
    slope_final_df['performance_trend_slope'] = slope_final_df['performance_trend_slope'].abs()
    #lenb= len(slope_final_df[['performance_data']])
    #print(lenb)
    #print(slope_final_df.shape)
    #slope_final_df.to_csv("Slope.csv")
    #slope_final_df = slope_final_df.rename({'0': 'performance_trend_slope'}, axis=1)
    #print(slope_final_df)
    return slope_final_df

def theil_reg(df, xcol, ycol):
   model = stats.theilslopes(df[ycol],df[xcol])
   return pd.Series(model)



def get_comparison_values(comparison_values):
    #neg_gap_df.reset_index(inplace=True)
    #neg_gap_measures = neg_gap_df[['http://example.com/slowmo#RegardingMeasure{BNode}']]
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
    comparison_values_df =comparison_values_df.rename({'http://example.com/slowmo#WithComparator{BNode}[0]': 'goal_comparator_node', 'http://example.com/slowmo#WithComparator{BNode}[1]': 'social_comparator_node'}, axis=1)
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
    final_df1['goal_comparator_size'] = final_df1['goal_comparator_size'].abs()
    final_df1['social_comparator_size'] = final_df1['SocialComparator']- final_df1['performance_data']
    final_df1['social_comparator_size'] = final_df1['social_comparator_size'].abs()
    #final_df1['goal_comparator_size'] = final_df1['performance_data'].fillna(0)
    #print(lenb)
    #final_df1.to_csv("final_df.csv")
    final_df1 = final_df1[['Measure_Name','goal_comparator_node','social_comparator_node','GoalComparator','SocialComparator','Passed_Count','Flagged_Count','Denominator','performance_data','goal_comparator_size','social_comparator_size']]
    #final_df1.to_csv("final_df.csv")
   # final_df1 = final_df1.rename({'http://example.com/slowmo#WithComparator{BNode}[0]': 'goal_comparator_node', 'http://example.com/slowmo#WithComparator{BNode}[1]': 'social_comparator_node'}, axis=1)
    #final_df1.to_csv("final_df.csv")
    #print(latest_measure_df.head())
    return final_df1

