import utils


class Reducer:
    reduce_output_file_path = "/reducer"

    def __init__(self, reduce_output_file_path="/reducer"):
        self.reduce_output_file_path = reduce_output_file_path

    # Input example: [('Hello', 1), ('Hello', 1), ('World', 1)]
    def reduce(self, words):
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
        utils.write_content(self.reduce_output_file_path, result)

    def get_output_path(self):
        return self.output_file_path
