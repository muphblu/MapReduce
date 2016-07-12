import xmlrpc.client


class Client:
    # Error definitions
    ERROR_NO_AVAILABLE_STORAGE = 'ERROR. No available storage'
    ERROR_NO_PATH = 'ERROR. No storage with this path'
    ONE_CHUNK_CHARS_COUNT = 1024

    def __init__(self, ip, port):
        # TODO: Replication of files in write - Naming server is responsible for replication
        # TODO: Size for a file and a directory. Like ls command
        self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
        self.connected_storages = []
        self.storage_coordinates = self.naming_server.get_storages_info()

        # Connecting to storages
        for storage in self.storage_coordinates:
            serverId = storage[0]
            # storage[1] is string 'ip:port'
            coordinates = storage[1].split(":")
            ip = coordinates[0]
            port = coordinates[1]
            storage_proxy = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            print('Storage with ip = ' + ip + ' is connected')

            storage_tuple = (serverId, storage_proxy)
            self.connected_storages.append(storage_tuple)

    # Client
    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        storage_list = self.naming_server.read(path)
        if len(storage_list) > 0:
            file_content = ''
            for index in range(len(storage_list)):
                server_id = storage_list[index][0]
                chunk_id = storage_list[index][1]
                file_content += self.connected_storages[server_id].read(chunk_id)
        else:
            # If there are no such path in storages then output error
            file_content = self.error_no_path
        return file_content

    # Client
    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        chunk_counts = self.get_chunk_counts(content)
        storage_list = self.naming_server.write(chunk_counts)

        if len(storage_list) > 0:
            for index in range(len(storage_list)):
                server_id = storage_list[index][0]
                chunk_id = storage_list[index][1]
                # self.storage_coordinates[index][1] is a chunk's id for index-th storage
                self.connected_storages[server_id].write(chunk_id, content)
        else:
            # If there are no available storage then output ERROR
            print(self.error_no_available_storage)

    # Client
    def delete_file(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Path in FS from where to delete
        :return:
        """
        self.naming_server.delete(path)

    # Client
    def create_directory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory path in FS
        :return:
        """
        self.naming_server.mkdir(path)

    # Client
    def delete_directory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory path in FS that is deleted
        :return:
        """
        self.naming_server.rmdir(path)

    # Client
    def size_query(self, path):
        """
        Size a query
        :param path: Directory path in FS that is deleted
        :return:
        """
        return self.naming_server(self, path)

    def list_directories(self, path):
        """
        Raise not a directory exception
        :param path: path to directory to list
        :return: return list of directories
        """
        pass

    def get_chunk_counts(self, content):
        """
        Returns the number of chunks to write
        :param content: content of a file to write
        :return: The number of chunks
        """
        words = content.split()
        one_chunk_content = ''
        chunk_counts = 0
        index = 0
        while index < len(words):
            while one_chunk_content <= self.ONE_CHUNK_CHARS_COUNT:
                one_chunk_content += words[index]
                index += 1
            chunk_counts += 1
        return chunk_counts

    def handle_user_input(self, user_input):
        """
        Handle user's input
        :param user_input: User's input through keyboard
        :return:
        """
        if 'read' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            result = self.read(path)
            print('The content of ' + path + ' is the following:')
            print(result)
        elif 'write' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            content = user_input("Input the content:")
            self.write(path[1], content)
        elif 'delete' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.delete_file(path[1])
        elif 'mkdir' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.create_directory(path[1])
        elif 'rmdir' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.delete_directory(path[1])


# TODO remove hardcoded variables
address = 'localhost'
port = 80
path = "path1"

client = Client(address, port)
print('Connection to naming server is established')

action = 0
while action.lower() != 'stop':
    action = input("Input one of the following commands:: \n"
                   "Stop - Stop the client \n"
                   "Read(<path of a file>) - Read a file \n"
                   "Write(<path of a file>) - Write\create a file \n"
                   "Delete(<path of a file>) - Delete a file \n"
                   "Mkdir(<path of a directory>) - Create a directory \n"
                   "Rmdir(<path of a directory>) - Delete a file or a directory \n"
                   "Size(<path of a file\directory>) - Size a file or files in a directory \n")
    client.handle_user_input(action)
