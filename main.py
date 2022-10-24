from src.graph import Node
from src.spark_pipline import read_files, process_df, save_as_tree
from src.ad_hoc import newspaper_with_most_different_drugs

if __name__ == "__main__":
    SRC_DATA = "./data/"
    DEST_DATA = "./result/"
    root = Node("root")

    #     #read files
    print("reading files ..")
    dict_dfs = read_files(SRC_DATA)

    #     # apply
    print("processing the dataframes !!")
    process_df(dict_dfs, DEST_DATA)

    print("saving result as json file ")
    save_as_tree(root, DEST_DATA + "final_result.csv", DEST_DATA)

    print(
        f"Le journal avec le plus de medicaments differents est: {newspaper_with_most_different_drugs(DEST_DATA + 'final_result.json')}"
    )
