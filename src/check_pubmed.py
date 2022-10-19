from unittest import result
import pyspark.sql.functions as F
from common import init_spark
import os

spark = init_spark("discover", driver_memory=4)

#load data
def load_original_data(
    file_path
    ):
    return spark.read.csv(
        file_path,
        encoding='utf-8',
        mode='FAILFAST',
        header=True,
        inferSchema=True
    )

#lower case
def lower_case(df, col):
    return df.withColumn(col, F.lower(F.col(col)))


def join(from_df, to_df, condition, how='inner'):
    return from_df.join(to_df, condition, how)

def rename_pubmed_columns(pubmed_df):
    return pubmed_df.withColumnRenamed('title', 'title_pubmed')\
            .withColumnRenamed('date', 'date_pubmed')\
            .withColumnRenamed('journal', 'journal_pubmed')

 
def pubMedPipline(
    drugs_path,
    pubmed_path,
    drug_col,
    pubmed_col
):

    drugs_df = load_original_data(drugs_path)
    pubmed_df = load_original_data(pubmed_path)
    drugs_df = lower_case(drugs_df, drug_col)
    pubmed_df = lower_case(pubmed_df, pubmed_col)
    pubmed_df = rename_pubmed_columns(pubmed_df)

    condition = pubmed_df.title_pubmed.contains(drugs_df.drug)
    join(drugs_df, pubmed_df, condition).toPandas().to_csv('result/res.csv', sep=',', header=True, index=False)





