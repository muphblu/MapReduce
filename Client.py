import xmlrpc.client


class Client:
    def __init__(self, ip, port):
        # TODO: create proxies for storage servers and one for naming server - Done
        # TODO: address for naming server in params - Done
        # TODO: receive storage servers' addresses from naming server
        #
        #
        # TODO: emulate console
        self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
        self.connected_storages = []
        self.storage_coordinates = []

    def connect_to_storage_servers(self):
        """
        Add proxies for all storages by coordinates received from naming server
        :return:
        """
        # IndexOutOfBound exception. Do not know how to solve
        storage_list = self.naming_server.read(path)
        for storage in storage_list:
            # storage[0] is a string - ip:port
            coordinates = storage[0].split(":")
            ip = coordinates[0]
            port = coordinates[1]
            storage = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            # TODO possible lost connection for storage variable
            self.connected_storages.append(storage)
            self.storage_coordinates.append(coordinates)
            print('Storage with ip =' + ip + 'is connected')

    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """

        file_content = ''

        for index in range(len(self.connected_storages)):
            # self.storage_coordinates[index][1] is a chunk's id for index-th storage
            chunk_id = self.storage_coordinates[index][1]
            file_content += self.connected_storages[index].read(chunk_id)

        return file_content;

    def write(self, path):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        pass

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
        pass

    def createDirectory(self, path):
        """
        Create a file in storage servers through Naming Server path
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
            print(result)
        elif 'write' in input.lower():
            path_to_file = input.split('(', 3)[1][:-1]
            self.write(path_to_file[1])


# TODO remove hardcoded variables
address = 'localhost'
port = 80
path = "path1"

client = Client(address, port)
print('Connection to naming server is established')
client.connect_to_storage_servers()

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
