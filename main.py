
from src.graph import Node
from src.spark_pipline import read_files, process_df, save_as_tree
from src.ad_hoc import newspaper_with_most_different_drugs

if __name__ == '__main__':
    root = Node('root')

    dict_pattern = {
    'drugs': '/home/sadi/Bureau/esmoz_servier/esmoz_servier/data/drugs.csv',
    'pubmed': '/home/sadi/Bureau/esmoz_servier/esmoz_servier/data/pubmed.csv',
    'clinical_trials': '/home/sadi/Bureau/esmoz_servier/esmoz_servier/data/clinical_trials.csv'
}

    #read files
    print("reading files ..")
    dict_dfs = read_files(dict_pattern)

    # apply process
    process_df(dict_dfs)

    root = Node('root')
    save_as_tree(root, 'result/final_result.csv')

    print(f"Le journal avec le plus de medicaments differents est: {newspaper_with_most_different_drugs('result/final_result.json')}")

 

