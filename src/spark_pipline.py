from email import header
from importlib.resources import path
from operator import index

from pickle import TRUE
from tkinter.tix import Tree
import pyspark.sql.functions as F
from common import init_spark
from pathlib import Path
import re
from functools import reduce

spark = init_spark("discover", driver_memory=4)


def read_files(dict_pattern):
    return {
        key: spark.read.csv(
            path=pattern,
            encoding='utf-8',
            mode='FAILFAST',
            header=True,
            inferSchema= True
        )
        for key, pattern in dict_pattern.items()
    }       

#lower case
def process_df(df_dict):
    #df_drug lower case drug column
    df_dict['drugs'] = df_dict['drugs'].withColumn('drug', F.lower(F.col('drug')))

    #df_pubmed lower case title column
    df_dict['pubmed'] = df_dict['pubmed'].withColumn('title', F.lower(F.col('title')))

    #clinical_trials lower case scientific_title column
    df_dict['clinical_trials'] = df_dict['clinical_trials'].withColumn('scientific_title', F.lower(F.col('scientific_title')))
    #-------------------------------------------------------------------------------------------------------------------#
    
    #add type col to pubmed and clinical dataframes
    df_dict['pubmed'] = df_dict['pubmed'].withColumn('type', F.lit('pubmed'))
    df_dict['clinical_trials'] = df_dict['clinical_trials'].withColumn('type', F.lit('clinic'))

    df_dict['clinical_trials'] = df_dict['clinical_trials'].withColumnRenamed('scientific_title', 'title')
    # concat df_pub_med and df_clinical_trials
    pubmed_clinical_df = df_dict['pubmed'].union(df_dict['clinical_trials'])
    #drop column id of pubmed_clinical df
    pubmed_clinical_df = pubmed_clinical_df.drop('id')

    #join pubmed_clinical_df with drugs df and save result as csv file
    print('saviiiing file')
    df_dict['drugs'].select('drug').join(pubmed_clinical_df, pubmed_clinical_df.title.contains(df_dict['drugs'].drug), 'inner').distinct()\
        .toPandas().to_csv('result/final_result.csv', header=True, index=False)


    



