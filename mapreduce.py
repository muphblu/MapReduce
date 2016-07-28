import re

import utils

MAP_OUTPUT_FILE_PATH = 'mapper'
REDUCE_OUTPUT_FILE_PATH = 'reducer'


def start_map(text, info_content=''):
    result = []
    print('Mapper result: ')
    for line in text:
        line = line.strip()
        # remove the symbols: !"?,.:;
        line = re.sub('[!"?,.:;]', "", line)
        words = line.split(" ")
        for w in words:
            result.append((w, 1))
            print('Mapper: ' + w + ':' + 1)
    utils.write_content(MAP_OUTPUT_FILE_PATH, result)
    return result


# Input example words: [('Hello', 1), ('Hello', 1), ('World', 1)]
def start_reduce(words, info_content=''):
    sorted_words = sorted(words)
    current_word = ""
    result = []
    values = []
    for i in sorted_words:
        if current_word != "" and i[0] != current_word:
            print("Reducer result: %s | %d" % (current_word, sum(values)))
            result.append((current_word, sum(values)))
            values = []
            current_word = i[0]
            values.append(i[1])
        elif i[0] == current_word or current_word == "":
            current_word = i[0]
            values.append(i[1])
    print("Reducer result: %s | %d" % (current_word, sum(values)))
    result.append((current_word, sum(values)))
    utils.write_content(REDUCE_OUTPUT_FILE_PATH, result)
