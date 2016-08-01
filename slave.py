import os
import xmlrpc.client

from threading import Thread
from xmlrpc.server import SimpleXMLRPCServer

import sys

from mapreduce import MAP_OUTPUT_FILE_PATH
from mapreduce import REDUCE_OUTPUT_FILE_PATH

import mapreduce
import utils
from storage_server import StorageServer
from mapreduce import Jobber
from master import FILES_ROOT

SLAVE_GENERAL_DIRECTORY_NAME = FILES_ROOT + 'slave_'


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
    def __init__(self, storage_id):
        # After initialization this will be /files/slave_{storage_id}
        self.slave_directory_path = ''
        # Init slave files structure
        self.init_slave_files_structure(storage_id)

        # Initializing a client
        try:
            master_address = utils.get_master_address()
            self.master = xmlrpc.client.ServerProxy('http://' + master_address[0] + ':' + str(master_address[1]))
            print('RPC for master is created')
        except WindowsError:
            print('Error in naming server')
            exit()

        # Connection to storage servers
        self.storage_servers = [utils.StorageServerInfo(server_info[0], server_info[1]) for server_info in
                                utils.get_slaves_info()]

        # Initializing a storage server. storage_id = [1, 4]
        slaves_info = utils.get_slaves_info()
        this_slave_info = slaves_info[storage_id - 1][1]

        self.jobber = Jobber(self, storage_id, master_address, self.slave_directory_path)
        self.storage_server = StorageServer(storage_id, this_slave_info, self.jobber)

        self.server = SimpleXMLRPCServer((this_slave_info[0], this_slave_info[1]), logRequests=False, allow_none=True)
        self.server.register_function(self.storage_server.read, "read")
        self.server.register_function(self.storage_server.write, "write")
        self.server.register_function(self.storage_server.delete, "delete")
        self.server.register_function(self.storage_server.replicate, "replicate")
        self.server.register_function(self.storage_server.ping, "ping")
        self.server.register_function(self.jobber.get_mapped_file)
        self.server.register_function(self.jobber.init_mapper)
        self.server.register_function(self.jobber.init_reducer)

        Thread(target=self.server.serve_forever).start()

        self.start()

    def init_slave_files_structure(self, slave_id):
        self.slave_directory_path = SLAVE_GENERAL_DIRECTORY_NAME + str(slave_id) + '/'
        if not os.path.isdir(FILES_ROOT):
            sys.exit('No root directory [' + FILES_ROOT + '], please create it first')
        if not os.path.isdir(self.slave_directory_path):
            os.mkdir(self.slave_directory_path)

    # ==========================================
    # Client API available for user
    # ==========================================
    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        try:
            chunk_info_list = self.master.read(path)
            if len(chunk_info_list) > 0:
                file_content = ''
                for index in range(len(chunk_info_list)):
                    chunk_info = utils.get_chuck_info(chunk_info_list[index])
                    main_server = list(filter(lambda x: x.id == chunk_info.main_server_id, self.storage_servers))[0]
                    file_content += main_server.proxy.read(chunk_info.chunk_name)
                    return file_content
        except:
            # If there are no such path in storages then output error
            return self.ERROR_NO_PATH

    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        chunks = self._get_chunks(content)
        size = len(content)
        chunk_info_list = self.master.write(path, size, len(chunks))

        # Sorting by chunk position
        chunk_info_list = sorted(chunk_info_list, key=lambda storage: storage['chunk_position'])
        if len(chunk_info_list) > 0:
            for index in range(len(chunk_info_list)):
                chunk_info = utils.get_chuck_info(chunk_info_list[index])

                main_server = list(filter(lambda x: x.id == chunk_info.main_server_id, self.storage_servers))[0]
                main_server.proxy.write(chunk_info.chunk_name, chunks[chunk_info.chunk_position])

                replica_server = list(filter(lambda x: x.id == chunk_info.replica_server_id, self.storage_servers))[0]
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
        result = self.master.delete(path)
        print(result)

    def create_directory(self, path):
        """
        Create a directory in storage servers through Naming Server path
        :param path: New directory path in FS
        :return:
        """
        result = self.master.mkdir(path)
        print(result)

    def delete_directory(self, path):
        """
        Delete a file in storage servers through Naming Server path
        :param path: Directory path in FS that is deleted
        :return: result string
        """
        result = self.master.rmdir(path)
        print(result)

    def size_query(self, path):
        """
        Size a query
        :param path: Directory path in FS that is deleted
        :return:
        """
        if self.master.get_type(path) == utils.DirFileEnum.File:
            return str(self.master.size(path))
        else:
            return 'N/A'

    def list_directories(self, path):
        """
        Raise not a directory exception - Handled by Naming server
        :param path: path to directory to list
        :return: return list of directories
        """
        result = self.master.list(path)
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
        self.master.start_job(path, mapper_content, reducer_content)
        print('Job is received successfully')

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
        elif 'writef' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            file_path = input("Input the source file path:")
            with open(file_path, mode='r') as file:
                content = file.read()
            if content is None:
                raise FileNotFoundError
            self.write(path, content)
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
                           "DoJob(<path of a file>) - Sends a job to job tracker \n"
                           "Writef(<path of a file>) - Write\create a file from existing file\n")
            self._handle_user_input(action)
