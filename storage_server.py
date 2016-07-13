import os
from xmlrpc.server import SimpleXMLRPCServer
import utils


class StorageServer:
    def __init__(self, server_id, address):
        """
        Storage server class
        should be started in separate thread
        :param address: tuple with network address and port
        """
        self.id = server_id
        self.other_servers = self.init_proxies(list(
            filter(lambda server: server[0] != self.id, utils.get_servers_info())))

        self.server = SimpleXMLRPCServer(address)
        self.server.register_function(self.read, "read")
        self.server.register_function(self.write, "write")
        self.server.register_function(self.delete, "delete")
        self.server.register_function(self.replicate, "replicate")
        self.server.serve_forever()

    def read(self, chunk_name):
        """
        Reads chunk from the storage server.
        Raises FileNotFoundError
        :param chunk_name: id of the chunk to be read from storage server
        :return: list of bytes
        """
        # TODO: put all files in separate directory
        if not os.path.exists(chunk_name):
            raise FileNotFoundError()
        with open(chunk_name, mode='r') as file:
            return file.read()

    def write(self, chunk_name, content):
        """
        Writes chunk to the storage server
        Raises FileExistsError
        :param chunk_name: id of the chunk to be written
        :param content: content of this chunk represented as list of bytes
        :return: status
        """
        with open(chunk_name, mode='x') as file:
            file.write(content)
        return True

    def delete(self, chunk_name):
        """
        Deletes chunk from storage storage server
        :param chunk_name: id of the chunk to be deleted
        :return: status
        """
        os.remove(chunk_name)
        return True

    def replicate(self, chunk_name, server_id):
        """
        Replicates particular chunk to particular StorageServer
        :param chunk_name: chunk to replicate
        :param server_id: id of the server where to replicate
        """
        server = list(filter(lambda x: x.id == server_id, self.other_servers))[0]
        server.proxy.write(chunk_name, self.read(chunk_name))

    # ===============================
    # Helpers
    # ===============================
    def init_proxies(self, servers_to_connect):
        return list(map(lambda x: utils.StorageServerInfo(x[0], x[1]), servers_to_connect))
