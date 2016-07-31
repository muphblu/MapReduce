import os
import xmlrpc.client

from threading import Thread
from xmlrpc.server import SimpleXMLRPCServer
from mapreduce import get_mapped_file

import mapreduce
import utils
from storage_server import StorageServer
from mapreduce import Jobber


class Slave:
    # ===============================
    # Error definitions
    # ===============================
    ERROR_NO_AVAILABLE_STORAGE = 'ERROR. No available storage'
    ERROR_NO_PATH = 'ERROR. No storage with this path'
    ONE_CHUNK_CHARS_COUNT = 1024

    # ===============================
    # Client
    # ===============================
    def __init__(self, ip, port, storage_id):
        # Initializing a client
        try:
            master_address = utils.get_master_address()
            self.master = xmlrpc.client.ServerProxy('http://' + master_address[0] + ':' + str(master_address[1]))
            print('Connection to naming server is established')
        except WindowsError:
            print('Error in naming server')
            exit()

        # Connection to storage servers
        self.storage_servers = [utils.StorageServerInfo(server_info[0], server_info[1]) for server_info in
                                utils.get_slaves_info()]

        # Initializing a storage server. storage_id = [1, 4]
        slaves_info = utils.get_slaves_info()
        this_slave_info = list(filter(lambda info: info[0] == storage_id, slaves_info))[0]

        self.storage_server = StorageServer(storage_id, this_slave_info[1])

        self.jobber = Jobber(storage_id, self.master)

        self.server = SimpleXMLRPCServer((ip, port), logRequests=False)
        self.server.register_function(self.storage_server.read, "read")
        self.server.register_function(self.storage_server.write, "write")
        self.server.register_function(self.storage_server.delete, "delete")
        self.server.register_function(self.storage_server.replicate, "replicate")
        self.server.register_function(self.storage_server.ping, "ping")
        self.server.register_function(get_mapped_file)
        self.server.register_function(self.jobber.init_mapper)
        self.server.register_function(self.jobber.init_reducer)

        self.start()

    # ==========================================
    # Client API available for user
    # ==========================================
    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        chunk_info_list = self.master.read(path)
        if len(chunk_info_list) > 0:
            file_content = ''
            for index in range(len(chunk_info_list)):
                chunk_info = utils.get_chuck_info(chunk_info_list[index])
                main_server = list(filter(lambda x: x[0] == chunk_info.main_server_id, self.storage_servers))[0][1]
                file_content += main_server.proxy.read(chunk_info.chunk_name)
        else:
            # If there are no such path in storages then output error
            file_content = self.ERROR_NO_PATH
        return file_content

    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        chunks = self._get_chunks(content)
        size = len(content)
        chunk_info_list = self.master.naming_server.write(path, size, len(chunks))

        # Sorting by chunk position
        chunk_info_list = sorted(chunk_info_list, key=lambda storage: storage['chunk_position'])
        if len(chunk_info_list) > 0:
            for index in range(len(chunk_info_list)):
                chunk_info = utils.get_chuck_info(chunk_info_list[index])

                main_server = \
                    list(filter(lambda x: x[0] == chunk_info.main_server_id, self.storage_servers))[0][1]
                main_server.proxy.write(chunk_info.chunk_name, chunks[chunk_info.chunk_position])
                replica_server = \
                    list(filter(lambda x: x[0] == chunk_info.replica_server_id, self.storage_servers))[0][1]
                replica_server.proxy.write(chunk_info.chunk_name, chunks[chunk_info.chunk_position])
                print(chunks[chunk_info.chunk_position] + ' is written to storages and replicated')
        else:
            # If there are no available storage then output ERROR
            print(self.ERROR_NO_AVAILABLE_STORAGE)

    def delete_file(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Path in FS from where to delete
        :return:
        """
        result = self.master.naming_server.delete(path)
        print(result)

    def create_directory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory path in FS
        :return:
        """
        result = self.master.naming_server.mkdir(path)
        print(result)

    def delete_directory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory path in FS that is deleted
        :return: result string
        """
        result = self.master.naming_server.rmdir(path)
        print(result)

    def size_query(self, path):
        """
        Size a query
        :param path: Directory path in FS that is deleted
        :return:
        """
        if self.master.naming_server.get_type(path) == utils.DirFileEnum.File:
            return str(self.master.naming_server.size(path))
        else:
            return 'N/A'

    def list_directories(self, path):
        """
        Raise not a directory exception - Handled by Naming server
        :param path: path to directory to list
        :return: return list of directories
        """
        result = self.master.naming_server.list(path)
        # This would print all the files and directories with sizes
        if isinstance(result, str):
            print(result)
        else:
            for file in result:
                size = self.size_query(path + '/' + str(file))
                print(file + '   ||   ' + str(size))

    # ===============================
    # Communication with job tracker
    # ===============================
    JOB_CONTENT_PATH = os.path.abspath(os.path.dirname(__file__)) + '\job_content.py'

    def start_the_job(self, path):
        """
        Send a job to job tracker
        :param path:
        :return:
        """
        mapper_content = utils.get_map_code()
        reducer_content = utils.get_reducer_code()
        self.master.naming_server.job_tracker.start_job(path, mapper_content, reducer_content)
        print('Job is received successfully')

    def start_map(self, file_path, info):
        """
        Receive a job from job tracker to execute
        :param info:
        :param file_path:
        :return:
        """
        print("Mapper is received from JT")
        file_content = self.read(file_path)
        print("Reading the file with path " + file_path)
        mapreduce.start_map(file_content, info)

    def start_reduce(self, words, info_content):
        """
        Execute a file of a job
        :param words:
        :param info_content:
        :return:
        """
        print("Reducer is received from JT")
        mapreduce.start_reduce(words, info_content)

    # ===============================
    # Helpers
    # ===============================
    def _get_chunks(self, content):
        """
        Return the number of chunks to write
        :param content: content of a file to write
        :return: The number of chunks
        """
        words = content.split()
        one_chunk_content = ''
        chunk_counts = 0
        index = 0
        result_chunks_content = []
        while index <= len(words):
            if index != len(words):
                if len(one_chunk_content + words[index]) <= self.ONE_CHUNK_CHARS_COUNT:
                    one_chunk_content += " " + words[index]
                    index += 1
                else:
                    result_chunks_content.append(one_chunk_content)
                    one_chunk_content = ''
                    chunk_counts += 1
            else:
                result_chunks_content.append(one_chunk_content)
                chunk_counts += 1
                break
        return result_chunks_content

    def _handle_user_input(self, user_input):
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
        elif 'dojob' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.start_the_job(path)

    def start(self):

        Thread(target=self.server.serve_forever).start()

        #
        # master_addr = utils.get_master_address()

        # address = master_addr[0]
        # port = master_addr[1]
        # storage_id = int(sys.argv[3])

        # client = Slave(address, port, storage_id)

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
                           "Size(<path of a file>) - Size of a file \n"
                           "DoJob(<path of a file>) - Sends a job to job tracker \n")
            self._handle_user_input(action)


slave = Slave('localhost', 8001, 1)
