import os
import re
from xmlrpc.server import SimpleXMLRPCServer

import utils

MAP_OUTPUT_FILE_PATH = 'mapper'
REDUCE_OUTPUT_FILE_PATH = 'reducer'


def get_mapped_file(server_id):
    with open(str(server_id), 'r') as file:
        content = file.read()
    return content


class Jobber:
    def __init__(self):
        servers_info = utils.get_slaves_info()
        self.servers = [utils.StorageServerInfo(server[0], server[1]) for server in servers_info]
        self.results = {}
        # self.mapper_server = SimpleXMLRPCServer(self.address, logRequests=False, allow_none=True)

    def init_results_dict(self, info):
        for server_id in info:
            self.results.setdefault(server_id, [])
            os.remove(server_id)

    def write_pair(self, pair):
        """
        Appends the given pair to the hashmap
        :param pair:
        :return:
        """
        self.results[hash(pair[0]) % len(self.results)].append(pair)

    def write_results_to_files(self):
        for key, val in self.results:
            file = open(str(key), 'a')
            for pair in val:
                file.write('%s\n' % str(pair))
            file.close()

    def start_map(self, text, info_content=''):
        # result = []
        print('Mapper result: ')
        for line in text:
            line = line.strip()
            # remove the symbols: !"?,.:;
            line = re.sub('[!"?,.:;]', "", line)
            words = line.split(" ")
            for w in words:
                self.write_pair((w, 1))
                # result.append((w, 1))
                print('Mapper: ' + w + ':' + 1)
        # utils.write_content(MAP_OUTPUT_FILE_PATH, result)
        # return result

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
