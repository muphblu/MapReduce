import pickle


class FileInfo(object):
    def __init__(self, path, size):
        self.size = size
        self.path = path

    def save_file(self):
        with open(self.path, mode='wb') as file:
            pickle.dump(self, file)

    @staticmethod
    def get_file(path):
        with open(path, mode='rb') as file:
            return pickle.load(file)