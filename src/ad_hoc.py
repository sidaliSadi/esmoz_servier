from collections import Counter
import json

def newspaper_with_most_different_drugs(file_path):
    """
    Input:
        file_path: the file resulted by the pipline (final_result.json)
    Output:
        the newspaper that mention the most different drugs and how many drugs
    """
    journaux = []
    with open(file_path) as f:
        json_df = json.load(f)
    for drug in json_df['children']:
        for info in drug['children']:
            if 'journal' in info.keys():
                journaux.append(info['journal'])
    return Counter(journaux).most_common(1)
