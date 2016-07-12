import pickle
from enum import Enum


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