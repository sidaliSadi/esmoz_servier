from ..src.ad_hoc import newspaper_with_most_different_drugs

def test_newspaper_with_most_different_drugs():
    correct_res = [('Psychopharmacology', 2)]
    path = './result/final_result.json'
    assert newspaper_with_most_different_drugs(path) == correct_res