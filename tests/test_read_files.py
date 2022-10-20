from ..src.spark_pipline import read_files


dict_pattern = {
    'drugs': './data/drugs.csv',
    'pubmed': './data/pubmed.csv',
    'clinical_trials': './data/clinical_trials.csv'
}

def test_newspaper_with_most_different_drugs():
    # correct_res = [('Psychopharmacology', 2)]
    path = '../result/final_result.json'
    correct_answer = ['atccode', 'drug']
    dict_dfs = read_files(dict_pattern)
    assert dict_dfs['drugs'].columns == correct_answer