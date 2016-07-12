import xmlrpc
from xmlrpc.server import SimpleXMLRPCServer


def get_own_address():
    return "localhost:8000"


def get_servers_addresses():
    return [
        (1, "localhost:8001"),
        (2, "localhost:8002"),
        (3, "localhost:8003"),
        (4, "localhost:8004")
    ]


def create_proxy(address_str):
    address = address_str.split(":")
    return xmlrpc.client.ServerProxy('http://' + address[0] + ':' + address[1])


class NamingServer:
    def __init__(self, port):

        # Naming server configuration
        address_str = get_own_address()
        address = address_str.split(":")
        self.server = SimpleXMLRPCServer((address[0], address[1]))
        # registering functions
        self.server.register_function(self.read)
        self.server.register_function(self.write)
        self.server.register_function(self.delete)
        self.server.register_function(self.size)
        self.server.register_function(self.list)
        self.server.register_function(self.mkdir)
        self.server.register_function(self.rmdir)
        self.server.register_function(self.get_type)
        self.server.register_function(self.get_storages_info)

        # Connection to storage servers
        self.storages = {server_info[0]: create_proxy(server_info[1]) for server_info in get_servers_addresses()}

    def read(self, path):
        """
        Read file from FS
        :param path: Path in FS from where to read
        :return: ordered list of tuples (server_id, chunk_id)
        """
        reply = []
        stub_tuple = (1, "some_chunk_id")
        # TODO: use reply.append(stub_tuple) cause this one doesn't work
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
        # TODO: use reply.append(stub_tuple) cause this one doesn't work
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

    def get_storages_info(self):
        """
        Provides list of storage servers addresses for the client
        :return: dictionary where key is server id value is server address as a string "127.0.0.1:8000"
        """
        return self.storages_addresses
