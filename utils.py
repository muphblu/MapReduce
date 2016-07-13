import pickle
from xmlrpc.client import ServerProxy
from collections import namedtuple
from enum import Enum

# Info about chunk:
# :param chunk_position: number position in file
# :param chunk_name: chunk name
# :param main_server_id: id of server where main copy stored
# :param replica_server_id: id of server where replica stored
ChunkInfo = namedtuple('ChunkInfo', ['chunk_position', 'chunk_name', 'main_server_id', 'replica_server_id'])


def get_own_address():
    return "localhost:8000"


def get_servers_info():
    return [
        (1, "localhost:8001"),
        (2, "localhost:8002"),
        (3, "localhost:8003"),
        (4, "localhost:8004")
    ]


class DirFileEnum(Enum):
    """
    Enum for provide info about path, is it directory, file, or neither
    """
    Error = 0
    Directory = 1
    File = 2


class FileInfo(object):
    """
    Helper for creating stubs of real DFS files at NamingServer's file system
    """
    def __init__(self, path, size, chunks_info_list):
        """
        :param path: path to file
        :param size: file size
        :param chunks_info_list: list of ChunkInfo objects
        """
        self.size = size
        self.path = path
        self.chunks = chunks_info_list

    def save_file(self):
        """Serializes and saves object to real file system, using path of DFS file"""
        with open(self.path, mode='wb') as file:
            pickle.dump(self, file)

    @staticmethod
    def get_file_info(path):
        """
        Method to create FIleInfo object using serialized FileInfo object on disk
        :return: FileInfo object
        """
        with open(path, mode='rb') as file:
            return pickle.load(file)


class StorageServerInfo:
    """
    Helper object that represents particular StorageServer at NamingServer
    """
    def __init__(self, server_id, address):
        """
        :param server_id:
        :param address: RPC network address in format domain_name:port or ip:port
        """
        self.id = server_id
        self.address = address
        self.proxy = self.init_proxy()

        # Helping structure to know which files stored on that server
        # contains only paths
        self.files = set()

    def init_proxy(self):
        address = self.address.split(":")
        return ServerProxy('http://' + address[0] + ':' + address[1])
