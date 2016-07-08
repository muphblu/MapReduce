from xmlrpc.server import SimpleXMLRPCServer


class StorageServer:
    def __init__(self):
        self.server = SimpleXMLRPCServer(("localhost", 8000))
        self.server.register_function(self.read, "read")
        self.server.register_function(self.write, "write")
        self.server.register_function(self.delete, "delete")
        # self.server.register_function(self.size, "size")
        self.server.serve_forever()
        pass

    def read(self, chunk_id):
        """
        Reads chunk from the storage server
        :param chunk_id: id of the chunk to be read from storage server
        :return: list of bytes
        """
        return "return from storage.read()"
        pass

    def write(self, chunk_id, content):
        """
        Writes chunk to the storage server
        :param chunk_id: id of the chunk to be written
        :param content: content of this chunk represented as list of bytes
        :return: status
        """
        return "return from storage.read()"

    def delete(self, chunk_id):
        """
        Deletes chunk from storage storage server
        :param chunk_id: id of the chunk to be deleted
        :return: status
        """
        return "return from storage.read()"

    # def size(self):
    #     return "return from storage.read()"