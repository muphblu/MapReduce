import os
import re
from ast import literal_eval
from threading import Thread

import utils

MAP_OUTPUT_FILE_PATH = 'mapper'
REDUCE_OUTPUT_FILE_PATH = 'reducer'


def start_map(jobber, text):  # , info_content=''):
    # result = []
    print('Mapper result: ')
    for line in text:
        line = line.strip()
        # remove the symbols: !"?,.:;
        line = re.sub('[!"?,.:;]', "", line)
        words = line.split(" ")
        for w in words:
            jobber.write_pair((w, 1))
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


def get_mapped_file(server_id):
    with open(str(server_id), 'r') as file:
        content = file.read()
    return content


class Jobber:
    def __init__(self, server_id):
        self.server_id = server_id
        servers_info = utils.get_slaves_info()
        self.servers = [utils.StorageServerInfo(server[0], server[1]) for server in servers_info]
        self.results = {}
        # self.mapper_server = SimpleXMLRPCServer(self.address, logRequests=False, allow_none=True)

    def init_results_dict(self, info):
        for server_id in info:
            self.results.setdefault(server_id, [])
            # todo form actual path to map results. Smth like "files/map_results/" + str(key)
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
            # todo form actual path to map results. Smth like "files/map_results/" + str(key)
            file = open(str(key), 'a')
            for pair in val:
                file.write('%s\n' % str(pair))
            file.close()

    def init_mapper(self, chunk_info_list, func_content):
        Thread(target=self.setup_mapper, args=(chunk_info_list, func_content)).start()

    def setup_mapper(self, chunk_info_list, func_content):
        exec(func_content)
        for index in range(len(chunk_info_list)):
            chunk_info = utils.get_chuck_info(chunk_info_list[index])
            main_server = list(filter(lambda x: x[0] == chunk_info.main_server_id, self.servers))[0][1]
            chunk_content = main_server.proxy.read(chunk_info.chunk_name)
            start_map(self, chunk_content)
        self.write_results_to_files()
        # todo inform JT about finished map on this node

    @staticmethod
    def split_to_list(data):
        result = []
        for line in data.split("\n"):
            if line is not "":
                new_tuple = literal_eval(line)
                result.append(new_tuple)
        return result

    def init_reducer(self, list_of_mappers, func_content):
        Thread(target=self.setup_reducer, args=(list_of_mappers, func_content)).start()

    def setup_reducer(self, list_of_mappers, func_content):
        exec(func_content)
        words = []
        for mapper in list_of_mappers:
            words = [line.strip() for line in self.servers[mapper].proxy.get_mapped_file(self.server_id)]
        start_reduce(words)
