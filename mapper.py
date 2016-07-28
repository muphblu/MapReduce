import re

import utils

OUTPUT_FILE_PATH = 'mapper'


def map(text):
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
    utils.write_content(OUTPUT_FILE_PATH, result)
    return result
