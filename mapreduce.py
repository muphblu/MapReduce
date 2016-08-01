import os
import re
import shutil
import hashlib
from ast import literal_eval
from threading import Thread
import xmlrpc.client

import sys

import time

MAP_OUTPUT_FILE_PATH = 'mapper/'
REDUCE_OUTPUT_FILE_PATH = 'reducer/'

import utils


def start_map(jobber, text):  # , info_content=''):
    words = re.split('\W+', text)
    for w in words:
        jobber.write_pair((w, 1))


# Input example words: [('Hello', 1), ('Hello', 1), ('World', 1)]
def start_reduce(jobber, pairs, info_content=''):
    intermediate_results_dist = {}
    results_list = []
    for item in pairs:
        intermediate_results_dist.setdefault(item[0], []).append(item[1])
    for key, val in intermediate_results_dist.items():
        word_count = 0
        for i in val:
            word_count += i
        jobber.reduce_results.append((key, word_count))
        # jobber.reduce_results = results_list


def split_into_words(string):
    return string.split()


class Jobber:
    def __init__(self, slave, server_id, master_address, slave_directory_path):
        self.server_id = server_id
        self.slave_directory_path = slave_directory_path
        self.slave = slave
        self.mapper_directory_path = slave_directory_path + MAP_OUTPUT_FILE_PATH
        self.reducer_directory_path = slave_directory_path + REDUCE_OUTPUT_FILE_PATH
        self.master_proxy = xmlrpc.client.ServerProxy('http://' + master_address[0] + ':' + str(master_address[1]))
        servers_info = utils.get_slaves_info()
        self.servers = [utils.StorageServerInfo(server[0], server[1]) for server in servers_info]
        self.map_results = {}
        self.reduce_results = []
        self.reducer_ids_list = []
        # self.mapper_server = SimpleXMLRPCServer(self.address, logRequests=False, allow_none=True)

    def get_mapped_file(self, server_id):
        mapped_file_path = self.mapper_directory_path + str(server_id)
        with open(mapped_file_path, 'r') as file:
            content = file.read()
        return self.split_to_list(content)

    def init_mapper_results_dict(self, reducer_id_list):
        if not os.path.isdir(self.slave_directory_path):
            sys.exit('directory [' + self.slave_directory_path + '] does not exist')

        if os.path.isdir(self.mapper_directory_path):
            shutil.rmtree(self.mapper_directory_path)
            time.sleep(1)
        os.mkdir(self.mapper_directory_path)

        for server_id in reducer_id_list:
            self.map_results.setdefault(server_id, [])
            path_to_results = self.mapper_directory_path + str(server_id)
            if os.path.isfile(path_to_results):
                os.remove(path_to_results)

    def write_pair(self, pair):
        """
        Appends the given pair to the hashmap
        :param pair:
        :return:
        """
        s = pair[0].encode('utf-8')
        reducer_index = int(hashlib.sha1(s).hexdigest(), 16) % len(self.map_results)
        self.map_results[self.reducer_ids_list[reducer_index]].append(pair)

    def write_results_to_files(self):
        for key, val in self.map_results.items():
            file = open(str(self.mapper_directory_path + str(key)), 'a')
            for pair in val:
                file.write('%s\n' % str(pair))
            file.close()

    def init_mapper(self, chunk_info_list, func_content, list_of_reducer_ids):
        Thread(target=self.setup_mapper, args=(chunk_info_list, func_content, list_of_reducer_ids)).start()

    def setup_mapper(self, chunk_info_list, func_content, list_of_reducer_ids):
        self.init_mapper_results_dict(list_of_reducer_ids)
        self.reducer_ids_list = list_of_reducer_ids
        exec(func_content)
        for index in range(len(chunk_info_list)):
            chunk_info = utils.get_chuck_info(chunk_info_list[index])
            main_server = list(filter(lambda x: x.id == chunk_info.main_server_id, self.servers))[0]
            chunk_content = main_server.proxy.read(chunk_info.chunk_name)
            start_map(self, chunk_content)
        self.write_results_to_files()
        self.master_proxy.map_finished(self.server_id)

    @staticmethod
    def split_to_list(data):
        result = []
        for line in data.split("\n"):
            if line is not "":
                new_tuple = literal_eval(line)
                result.append(new_tuple)
        return result

    def write_reduce_results_to_file(self):
        file = open(str(self.reducer_directory_path + 'results'), 'a')
        for pair in self.reduce_results:
            file.write('%s\n' % str(pair))
        file.close()

    def write_reduce_results_to_dfs(self):
        with open(self.reducer_directory_path + 'results', 'r') as file:
            string = file.read()
        self.slave.write('results/result_' + str(self.server_id), string)

    def init_reducer_dir(self):
        if os.path.isdir(self.reducer_directory_path):
            shutil.rmtree(self.reducer_directory_path)
            time.sleep(1)
        os.mkdir(self.reducer_directory_path)

    def init_reducer(self, list_of_mappers, func_content):
        Thread(target=self.setup_reducer, args=(list_of_mappers, func_content)).start()

    def setup_reducer(self, list_of_mappers, func_content):
        self.init_reducer_dir()
        exec(func_content)
        words = []
        for mapper_id in list_of_mappers:
            mapper = list(filter(lambda x: x.id == mapper_id, self.servers))[0]
            words.extend(mapper.proxy.get_mapped_file(self.server_id))
            # words = [line.strip() for line in self.servers[mapper].proxy.get_mapped_file(self.server_id)]
        start_reduce(self, words)
        self.write_reduce_results_to_file()
        self.master_proxy.reduce_finished(self.server_id,
                                          'files/slave_' + str(self.server_id) + '/reducer/results')
