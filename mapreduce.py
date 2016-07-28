import re


class MapReduce:
    map_output_file_path = "/mapper"
    reduce_output_file_path = "/reducer"

    def __init__(self, map_output_file_path='/mapper', reduce_output_file_path = "/reducer"):
        self.map_output_file_path = map_output_file_path
        self.reduce_output_file_path = reduce_output_file_path

    def map(self, text):
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
        self.write_output(self.map_output_file_path, result)
        return result

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
        self.write_output(self.reduce_output_file_path, result)

    def write_output(self, path, result):
        result_path = self.root_directory + '/' + path
        with open(result_path, mode='x') as file:
            file.write(result)
        print('The mapper result is written')

    def get_map_output_path(self):
        return self.output_file_path
