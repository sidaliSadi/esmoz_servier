from functools import reduce
import pyspark.sql.functions as F
from .common import init_spark
import csv, json
import os
from pyspark.sql import DataFrame
from functools import reduce

spark = init_spark("discover", driver_memory=4)


def read_files(path_dir):
    """
    Input:
        dict_pattern: dictionary {'df_name': 'path to file'}
    Output:
        dictionary {'df_name': pyspark dataframe}
    """
    # return spark.read.option("multiline", "true").json("./data/pubmed*.json")
    res_dict = {"clinical_trials": [], "drugs": [], "pubmed": []}

    file_type = ["clinical_trials", "drugs", "pubmed"]
    for ft in file_type:
        print(
            path_dir + ft + "*.csv",
        )
        try:
            res_dict[ft].append(
                spark.read.csv(
                    path=path_dir + ft + "*.csv",
                    encoding="utf-8",
                    mode="FAILFAST",
                    header=True,
                    inferSchema=True,
                )
            )
        except:
            pass
        try:
            res_dict[ft].append(
                spark.read.option("multiline", "true").json(path_dir + ft + "*.json")
            )
        except:
            pass
    return {key: reduce(DataFrame.unionByName, dfs) for key, dfs in res_dict.items()}


def process_df(df_dict):
    """
    Input:
        dictionary of dataframes {df_name: dataframe}
    Output:
    save the file as csv
    """
    # df_drug lower case drug column
    df_dict["drugs"] = df_dict["drugs"].withColumn("drug", F.lower(F.col("drug")))

    # df_pubmed lower case title column
    df_dict["pubmed"] = df_dict["pubmed"].withColumn("title", F.lower(F.col("title")))

    # clinical_trials lower case scientific_title column
    df_dict["clinical_trials"] = df_dict["clinical_trials"].withColumn(
        "scientific_title", F.lower(F.col("scientific_title"))
    )
    # -------------------------------------------------------------------------------------------------------------------#

    # add type col to pubmed and clinical dataframes
    df_dict["pubmed"] = df_dict["pubmed"].withColumn("type", F.lit("pubmed"))
    df_dict["clinical_trials"] = df_dict["clinical_trials"].withColumn(
        "type", F.lit("clinic")
    )

    df_dict["clinical_trials"] = df_dict["clinical_trials"].withColumnRenamed(
        "scientific_title", "title"
    )
    # concat df_pub_med and df_clinical_trials
    pubmed_clinical_df = df_dict["pubmed"].union(df_dict["clinical_trials"])
    # drop column id of pubmed_clinical df
    pubmed_clinical_df = pubmed_clinical_df.drop("id")

    # join pubmed_clinical_df with drugs df and save result as csv file
    print("saviiiing file")
    df_dict["drugs"].select("drug").join(
        pubmed_clinical_df,
        pubmed_clinical_df.title.contains(df_dict["drugs"].drug),
        "inner",
    ).distinct().toPandas().to_csv("result/final_result.csv", header=True, index=False)


def save_as_tree(root, file_path):
    """
    Input:
        root : instance of Node represent the root of our tree representation
        file_path: the file resulted by the pipline (final_result.csv)
    Output:
        Json file as tree
    """
    with open(f"{file_path}", "r") as f:
        reader = csv.reader(f)
        for rid, row in enumerate(reader):
            if rid == 0:
                continue
            drug, title, date, journal, type = row
            root.child(drug, "drug", cdate=None).child(title, type, date)
            root.child(drug, "drug", cdate=None).child(journal, "journal", date)
            # root.child(drug).child(journal_pubmed)
    with open("result/final_result.json", "w") as f:
        json.dump(root.as_dict(), f)
