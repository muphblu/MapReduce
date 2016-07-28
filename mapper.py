import re

import utils


class Mapper:
    map_output_file_path = "/mapper"

    def __init__(self, map_output_file_path='/mapper'):
        self.map_output_file_path = map_output_file_path

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
        utils.write_content(self.map_output_file_path, result)
        return result

    def get_output_path(self):
        return self.output_file_path
