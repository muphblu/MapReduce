import os
from xmlrpc.server import SimpleXMLRPCServer


class StorageServer:
    def __init__(self, address):
        """
        Storage server class
        should be started in separate thread
        :param address: tuple with network address and port
        """
        self.server = SimpleXMLRPCServer(address)
        self.server.register_function(self.read, "read")
        self.server.register_function(self.write, "write")
        self.server.register_function(self.delete, "delete")
        self.server.serve_forever()

    def read(self, chunk_id):
        """
        Reads chunk from the storage server.
        Raises FileNotFoundError
        :param chunk_id: id of the chunk to be read from storage server
        :return: list of bytes
        """
        # TODO: put all files in separate directory
        if not os.path.exists(chunk_id):
            raise FileNotFoundError()
        with open(chunk_id, mode='r') as file:
            return file.read()

    def write(self, chunk_id, content):
        """
        Writes chunk to the storage server
        Raises FileExistsError
        :param chunk_id: id of the chunk to be written
        :param content: content of this chunk represented as list of bytes
        :return: status
        """
        with open(chunk_id, mode='x') as file:
            file.write(content)
        return True

    def delete(self, chunk_id):
        """
        Deletes chunk from storage storage server
        :param chunk_id: id of the chunk to be deleted
        :return: status
        """
        os.remove(chunk_id)
        return True
