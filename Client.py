import xmlrpc.client
import random


class Client:
    # Error definitions
    error_no_available_storage = 'ERROR. No available storage'
    error_no_path = 'ERROR. No storage with this path'

    def __init__(self, ip, port):
        # TODO: create proxies for storage servers and one for naming server - Done
        # TODO: address for naming server in params - Done
        # TODO: receive storage servers' addresses from naming server - Done
        # TODO: emulate console - Done
        self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
        self.connected_storages = []
        self.storage_coordinates = []
        self.chunk_ids = []

    def connect_to_storage_servers(self, path):
        """
        Add proxies for all storages by coordinates received from naming server
        :return:
        """
        self.connected_storages = []
        self.storage_coordinates = []
        self.chunk_ids = []
        storage_list = self.naming_server.read(path)
        for storage in storage_list:
            # storage[0] is a string - ip:port
            coordinates = storage[0].split(":")
            ip = coordinates[0]
            port = coordinates[1]
            storage_proxy = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            # TODO possible lost connection for storage variable
            self.connected_storages.append(storage_proxy)
            self.storage_coordinates.append(coordinates)
            # storage[1] is a chunk id
            self.chunk_ids.append(storage[1])
            print('Storage with ip = ' + ip + ' is connected')
        if len(storage_list) > 0:
            return True
        else:
            return False

    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        file_content = ''
        # Establish connection to all storage servers, which contain file with path
        if self.connect_to_storage_servers(path):
            for index in range(len(self.connected_storages)):
                # self.storage_coordinates[index][1] is a chunk's id for index-th storage
                chunk_id = self.chunk_ids[index]
                file_content += self.connected_storages[index].read(chunk_id)
        else:
            # If there are no such path in storages then output error
            file_content = self.error_no_path
        return file_content

    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        # Establish connection to all storage servers, which contain file with path
        if self.connect_to_storage_servers(path):
            for index in range(len(self.connected_storages)):
                # self.storage_coordinates[index][1] is a chunk's id for index-th storage
                chunk_id = random.randrange(1, 30000, 1)
                self.connected_storages[index].write(chunk_id, content)
        else:
            # If there are no available storage then output ERROR
            print(self.error_no_available_storage)

    def createFile(self, path, file_name):
        """
        Create a file in storage servers through Naming Server path
        :param path: Path in FS where to create
        :param file_name: name of a file in a path
        :return:
        """
        pass

    def deleteFile(self, path, file_name):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Path in FS from where to delete
        :param file_name: name of a file in a path
        :return:
        """
        self.connect_to_storage_servers(path)
        for index in range(len(self.connected_storages)):
            # self.storage_coordinates[index][1] is a chunk's id for index-th storage
            chunk_id = self.chunk_ids[index]
            self.connected_storages[index].delete(chunk_id)

    def createDirectory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory name in FS
        :return:
        """
        pass

    def deleteDirectory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory name in FS that is deleted
        :return:
        """
        pass

    def sizeQuery(self, path):
        """
        Size a query
        :param path: Directory name in FS that is deleted
        :return:
        """
        pass

    def handleUserInput(self, input):
        """
        Handles user's input
        :param input: User's input through keyboard
        :return:
        """
        if 'read' in input.lower():
            path_to_file = input.split('(', 3)[1][:-1]
            result = self.read(path_to_file)
            print('The content of ' + path_to_file + ' is the following:')
            print(result)
        elif 'write' in input.lower():
            path_to_file = input.split('(', 3)[1][:-1]
            content = input("Input the content:")
            self.write(path_to_file[1], content)

# TODO remove hardcoded variables
address = 'localhost'
port = 80
path = "path1"

client = Client(address, port)
print('Connection to naming server is established')

action = 0
while action != 'stop':
    action = input("Input the following commands:: \n"
                   "Stop - Stop the client \n"
                   "Read(<path of a file>) - Read a file \n"
                   "Write(<path of a file>) - Write a file \n"
                   "Create(<path of a file\directory>) - Create a file or a directory \n"
                   "Delete(<path of a file\directory>) - Delete a file or a directory \n"
                   "Size(<path of a file\directory>) - Size a file or files in a directory \n")
    client.handleUserInput(action)
