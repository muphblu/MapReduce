import xmlrpc.client
import random


class Client:
    # Error definitions
    ERROR_NO_AVAILABLE_STORAGE = 'ERROR. No available storage'
    ERROR_NO_PATH = 'ERROR. No storage with this path'
    ONE_CHUNK_CHARS_COUNT = 1024

    def __init__(self, ip, port):
        # TODO: Replication of files in write - Naming server is responsible for replication
        # TODO: Delete a file
        # TODO: Size for a file and a directory. Like ls command
        self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
        self.connected_storages = []
        self.storage_coordinates = []
        self.chunk_ids = []

    def connect_to_storage_servers_for_read(self, path):
        """
        Add proxies for all storages by coordinates received from naming server with reading purpose
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
            self.connected_storages.append(storage_proxy)
            self.storage_coordinates.append(coordinates)
            # storage[1] is a chunk id
            self.chunk_ids.append(storage[1])
            print('Storage with ip = ' + ip + ' is connected')
        if len(storage_list) > 0:
            return True
        else:
            return False

    def connect_to_storage_servers_for_write(self, path, content):
        """
        Add proxies for all storages by coordinates received from naming server with writing purpose
        :return:
        """
        self.connected_storages = []
        self.storage_coordinates = []
        self.chunk_ids = []

        """Python's default encoding is the 'ascii' encoding
        In ASCII, each character is represented by one byte
        Therefore, one chunk is 1024 characters"""
        #TODO one word not on different chunks
        count_chunks = len(content) / self.ONE_CHUNK_CHARS_COUNT + 1

        storage_list = self.naming_server.write(path, count_chunks)
        for storage in storage_list:
            # storage[0] is a string - ip:port
            coordinates = storage[0].split(":")
            ip = coordinates[0]
            port = coordinates[1]
            storage_proxy = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            # TODO consider fixing possible lost connection for storage variable
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
        if self.connect_to_storage_servers_for_read(path):
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
        if self.connect_to_storage_servers_for_write(path, content):
            for index in range(len(self.connected_storages)):
                chunk_id = self.chunk_ids[index]
                # self.storage_coordinates[index][1] is a chunk's id for index-th storage
                self.connected_storages[index].write(chunk_id, content)
        else:
            # If there are no available storage then output ERROR
            print(self.error_no_available_storage)

    def delete_file(self, path, file_name):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Path in FS from where to delete
        :param file_name: name of a file in a path
        :return:
        """
        """self.connect_to_storage_servers(path)
        for index in range(len(self.connected_storages)):
            # self.storage_coordinates[index][1] is a chunk's id for index-th storage
            chunk_id = self.chunk_ids[index]
            self.connected_storages[index].delete(chunk_id)"""
        pass

    def create_directory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory name in FS
        :return:
        """
        self.naming_server.mkdir(path)

    def delete_directory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory name in FS that is deleted
        :return:
        """
        self.naming_server.rmdir(path)

    def size_query(self, path):
        """
        Size a query
        :param path: Directory name in FS that is deleted
        :return:
        """
        pass

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
while action != 'stop':
    action = input("Input one of the following commands:: \n"
                   "Stop - Stop the client \n"
                   "Read(<path of a file>) - Read a file \n"
                   "Write(<path of a file>) - Write\create a file \n"
                   "Delete(<path of a file>) - Delete a file \n"
                   "Mkdir(<path of a directory>) - Create a directory \n"
                   "Rmdir(<path of a directory>) - Delete a file or a directory \n"
                   "Size(<path of a file\directory>) - Size a file or files in a directory \n")
    client.handle_user_input(action)
