from xmlrpc.server import SimpleXMLRPCServer


class NamingServer:
    def __init__(self, port):
        self.server = SimpleXMLRPCServer(('localhost', port))

        self.server.register_function(self.read)
        self.server.register_function(self.write)
        self.server.register_function(self.delete)
        self.server.register_function(self.size)
        self.server.register_function(self.list)
        self.server.register_function(self.mkdir)
        self.server.register_function(self.rmdir)
        self.server.register_function(self.get_type)

    def read(self, path):
        """
        Read file from FS
        :param path: Path in FS from where to read
        :return: ordered list of tuples (server_id, chunk_id)
        """
        reply = []
        stub_tuple = (1, "some_chunk_id")
        reply[1] = stub_tuple
        return reply

    def write(self, count_chunks):
        """
        Write file to FS
        :param count_chunks: Number of chunks to write
        :return: ordered list of tuples (??)
        """
        reply = []
        stub_tuple = (1, "some_chunk_id")
        reply[1] = stub_tuple
        return reply

    def delete(self, path):
        pass

    def size(self, path):
        pass

    def list(self, path):
        pass

    def mkdir(self, path):
        pass

    def rmdir(self, path):
        pass

    def get_type(self, path):
        pass
