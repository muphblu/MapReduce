import os
import shutil
from threading import Thread
from xmlrpc.server import SimpleXMLRPCServer

import time

import sys

import utils


class StorageServer:
    def __init__(self, server_id, slave_info, jobber):
        """
        Storage server class
        should be started in separate thread
        :param address: tuple with network address and port
        """
        self.id = server_id
        self.jobber = jobber
        self.root_directory = "files/storage" + str(server_id)
        self.other_servers = self.init_proxies(list(
            filter(lambda server: server[0] != self.id, utils.get_slaves_info())))

        # reset storage directory
        if os.path.isdir(self.root_directory):
            shutil.rmtree(self.root_directory)
        time.sleep(1)
        os.mkdir(self.root_directory)

        # ip = slave_info[0]
        # port = slave_info[1]
        # self.storage_server = SimpleXMLRPCServer((ip, port), logRequests=False)
        # self.storage_server.register_function(self.read, "read")
        # self.storage_server.register_function(self.write, "write")
        # self.storage_server.register_function(self.delete, "delete")
        # self.storage_server.register_function(self.replicate, "replicate")
        # self.storage_server.register_function(self.ping, "ping")
        # self.storage_server.register_function(get_mapped_file)
        # self.storage_server.register_function(self.jobber.init_mapper)
        # self.storage_server.register_function(self.jobber.init_reducer)
        #
        # Thread(target=self.storage_server.serve_forever).start()
        print('Storage server is started')

    # ==========================================
    # RPC API
    # ==========================================
    def read(self, chunk_name):
        """
        Reads chunk from the storage server.
        Raises FileNotFoundError
        :param chunk_name: id of the chunk to be read from storage server
        :return: list of bytes
        """
        result_path = self.root_directory + '/' + chunk_name
        if not os.path.exists(result_path):
            raise FileNotFoundError()
        with open(result_path, mode='r') as file:
            return file.read()

    def write(self, chunk_name, content):
        """
        Writes chunk to the storage server
        Raises FileExistsError
        :param chunk_name: id of the chunk to be written
        :param content: content of this chunk represented as list of bytes
        :return: status
        """
        result_path = self.root_directory + '/' + chunk_name
        with open(result_path, mode='x') as file:
            file.write(content)
        return True

    def delete(self, chunk_name):
        """
        Deletes chunk from storage storage server
        :param chunk_name: id of the chunk to be deleted
        :return: status
        """
        result_path = self.root_directory + '/' + chunk_name
        os.remove(result_path)
        return True

    def replicate(self, chunk_name, server_id):
        """
        Replicates particular chunk to particular StorageServer
        :param chunk_name: chunk to replicate
        :param server_id: id of the server where to replicate
        """
        server = list(filter(lambda x: x.id == server_id, self.other_servers))[0]
        return server.proxy.write(chunk_name, self.read(chunk_name))

    def ping(self):
        return True

    # ===============================
    # Helpers
    # ===============================
    def init_proxies(self, servers_to_connect):
        return list(map(lambda x: utils.StorageServerInfo(x[0], x[1]), servers_to_connect))

    # ===============================
    # Getter for external classes
    # ===============================
    # Not used for now
    def get_other_storage_proxies(self):
        return self.other_servers


def get_server_addr(server_id):
    for x in utils.get_slaves_info():
        if x[0] == server_id:
            return x[1]


# server_id = 1
# if sys.argv[3] is not None:
#     server_id = int(sys.argv[3])
#
# address = get_server_addr(server_id)
# StorageServer(server_id, address)
