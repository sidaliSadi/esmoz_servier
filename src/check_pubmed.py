import pyspark.sql.functions as F
from common import init_spark


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
    return df.withColumn(col+'_', F.lower(F.col(col)))\
    .drop(col)

