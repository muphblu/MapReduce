import re

import utils

MAP_OUTPUT_FILE_PATH = 'mapper'


def start_map(jobber, text):
    result = []
    print('Mapper result: ')
    for line in text:
        line = line.strip()
        # remove the symbols: !"?,.:;
        line = re.sub('[!"?,.:;]', "", line)
        words = line.split(" ")
        for w in words:
            jobber.write_pair((w, 1))
            result.append((w, 1))
            print('Mapper: ' + w + ':' + 1)
    utils.write_content(MAP_OUTPUT_FILE_PATH, result)
    return result
