# Input example words: [('Hello', 1), ('Hello', 1), ('World', 1)]
def start_reduce(jobber, pairs, info_content=''):
    intermediate_results_dist = {}
    results_list = []
    for item in pairs:
        intermediate_results_dist.setdefault(item[0], []).append(item[1])
    for key, val in intermediate_results_dist:
        word_count = 0
        for i in val:
            word_count += i
        results_list.append((key, word_count))
    jobber.reduce_results = results_list
