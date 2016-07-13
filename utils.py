import pickle
from xmlrpc.client import ServerProxy
from collections import namedtuple
from enum import Enum

ChunkInfo = namedtuple('ChunkInfo', ['chunk_number', 'chunk_name', 'main_server_id', 'replica_server_id'])


class DirFileEnum(Enum):
    Error = 0
    Directory = 1
    File = 2


class FileInfo(object):
    def __init__(self, path, size, chunks):
        self.size = size
        self.path = path
        self.chunks = chunks

    def save_file(self):
        with open(self.path, mode='wb') as file:
            pickle.dump(self, file)

    @staticmethod
    def get_file(path):
        with open(path, mode='rb') as file:
            return pickle.load(file)


class StorageServerInfo:
    def __init__(self, server_id, address):
        self.id = server_id
        self.address = address
        self.proxy = self.init_proxy()
        # Helping structure to know which files stored on that server
        # contains only paths
        self.files = set()

    def init_proxy(self):
        address = self.address.split(":")
        return ServerProxy('http://' + address[0] + ':' + address[1])
