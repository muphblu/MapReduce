import utils

OUTPUT_FILE_PATH = 'reducer'


# Input example words: [('Hello', 1), ('Hello', 1), ('World', 1)]
def start_reducer(words):
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
    utils.write_content(OUTPUT_FILE_PATH, result)
