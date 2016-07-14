import xmlrpc.client
import utils
import sys


class Client:
    # ===============================
    # Error definitions
    # ===============================
    ERROR_NO_AVAILABLE_STORAGE = 'ERROR. No available storage'
    ERROR_NO_PATH = 'ERROR. No storage with this path'
    ONE_CHUNK_CHARS_COUNT = 1024

    # ===============================
    # Client
    # ===============================
    def __init__(self, ip, port):
        try:
            self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            self.connected_storages = []
            self.storage_coordinates = self.naming_server.get_storages_info()
            print('Connection to naming server is established')

            # Connecting to storages
            for storage in self.storage_coordinates:
                server_id = storage[0]
                # storage[1] is string 'ip:port'
                coordinates = storage[1].split(":")
                ip = coordinates[0]
                port = coordinates[1]
                storage_proxy = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
                print('Storage with ip = ' + ip + ' is connected')

                storage_tuple = (server_id, storage_proxy)
                self.connected_storages.append(storage_tuple)
        except WindowsError:
            print('Unavailable naming server')
            exit()

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

    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        chunk_counts = self.get_chunk_counts(content)
        size = self.naming_server.size(path)
        storage_list = self.naming_server.write(path, size, chunk_counts)

        # Sorting by chunk position
        storage_list = sorted(storage_list, key=lambda storage: storage['chunk_position'])
        if len(storage_list) > 0:
            for index in range(len(storage_list)):
                storage = utils.get_chuck_info(storage_list[index])

                main_server = self.connected_storages[storage.main_server_id][1]
                main_server.write(storage.chunk_name, content)
                replica_server = self.connected_storages[storage.replica_server_id][1]
                replica_server.write(storage.chunk_name, content)
                print(content + ' is written to storages and replicated')
        else:
            # If there are no available storage then output ERROR
            print(self.error_no_available_storage)

    def delete_file(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Path in FS from where to delete
        :return:
        """
        result = self.naming_server.delete(path)
        print(result)

    def create_directory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory path in FS
        :return:
        """
        result = self.naming_server.mkdir(path)
        print(result)

    def delete_directory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory path in FS that is deleted
        :return: result string
        """
        result = self.naming_server.rmdir(path)
        print(result)

    def size_query(self, path):
        """
        Size a query
        :param path: Directory path in FS that is deleted
        :return:
        """
        return self.naming_server.size(path)

    def list_directories(self, path):
        """
        Raise not a directory exception - Handled by Naming server
        :param path: path to directory to list
        :return: return list of directories
        """
        result = self.naming_server.list(path)
        # This would print all the files and directories with sizes
        if isinstance(result, str):
            print(result)
        else:
            for file in result:
                size = self.naming_server.size(path + '/' + file)
                print(file + '   ||   ' + size)

    # ===============================
    # Helpers
    # ===============================
    def get_chunk_counts(self, content):
        """
        Return the number of chunks to write
        :param content: content of a file to write
        :return: The number of chunks
        """
        words = content.split()
        one_chunk_content = ''
        chunk_counts = 0
        index = 0
        while index <= len(words):
            if index != len(words):
                if len(one_chunk_content + words[index]) <= self.ONE_CHUNK_CHARS_COUNT:
                    one_chunk_content += words[index]
                    index += 1
                else:
                    one_chunk_content = ''
                    chunk_counts += 1
            else:
                chunk_counts += 1
                break
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
            content = input("Input the content:")
            self.write(path, content)
        elif 'delete' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.delete_file(path)
        elif 'mkdir' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.create_directory(path)
        elif 'size' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            size = self.size_query(path)
            print('Size = ' + size)
        elif 'list' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            print(' File name  ||  Size ')
            self.list_directories(path)
        elif 'rmdir' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.delete_directory(path)


address = sys.argv[1]
port = int(sys.argv[2])

client = Client(address, port)

action = ''
while action.lower() != 'stop':
    action = input("Input one of the following commands:: \n"
                   "Stop - Stop the client \n"
                   "Read(<path of a file>) - Read a file \n"
                   "Write(<path of a file>) - Write\create a file \n"
                   "Delete(<path of a file>) - Delete a file \n"
                   "Mkdir(<path of a directory>) - Create a directory \n"
                   "Rmdir(<path of a directory>) - Delete a file or a directory \n"
                   "List(<directory>) - List files in a directory with sizes \n"
                   "Size(<path of a file>) - Size of a file \n")
    client.handle_user_input(action)
