import os
import xmlrpc.client

import sys
from threading import Thread

import utils
from mapper import Mapper
from reducer import Reducer
from storage_server import StorageServer
from job import Job


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
            self.naming_server = xmlrpc.client.ServerProxy('http://' + ip + ':' + str(port))
            print('Connection to naming server is established')
        except WindowsError:
            print('Error in naming server')
            exit()

        # Initializing mapper and reducer
        self.mapper = Mapper()
        self.reducer = Reducer()

        # Initializing a storage server. storage_id = [1, 4]
        self.storage_server = StorageServer(storage_id, ("localhost", 8000 + storage_id))
        Thread(target=self.storage_server.serve_forever).start()

    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        chunk_info_list = self.naming_server.read(path)
        if len(chunk_info_list) > 0:
            file_content = ''
            for index in range(len(chunk_info_list)):
                chunk_info = utils.get_chuck_info(chunk_info_list[index])
                main_server = list(filter(lambda x: x[0] == chunk_info.main_server_id, self.connected_storages))[0][1]
                file_content += main_server.read(chunk_info.chunk_name)
        else:
            # If there are no such path in storages then output error
            file_content = self.error_no_path
        return file_content

    def write(self, path, content):
        """
        Write file to storage servers through path received by Naming Server
        :param path: Path in FS from where to write
        """
        chunks = self.get_chunks(content)
        size = len(content)
        chunk_info_list = self.naming_server.write(path, size, len(chunks))

        # Sorting by chunk position
        chunk_info_list = sorted(chunk_info_list, key=lambda storage: storage['chunk_position'])
        if len(chunk_info_list) > 0:
            for index in range(len(chunk_info_list)):
                chunk_info = utils.get_chuck_info(chunk_info_list[index])

                main_server_proxy = \
                    list(filter(lambda x: x[0] == chunk_info.main_server_id, self.connected_storages))[0][1]
                main_server_proxy.write(chunk_info.chunk_name, chunks[chunk_info.chunk_position])
                replica_server_proxy = \
                    list(filter(lambda x: x[0] == chunk_info.replica_server_id, self.connected_storages))[0][1]
                replica_server_proxy.write(chunk_info.chunk_name, chunks[chunk_info.chunk_position])
                print(chunks[chunk_info.chunk_position] + ' is written to storages and replicated')
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
        if self.naming_server.get_type(path) == utils.DirFileEnum.File:
            return str(self.naming_server.size(path))
        else:
            return 'N/A'

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
                size = self.size_query(path + '/' + str(file))
                print(file + '   ||   ' + str(size))

    # ===============================
    # Communication with job tracker
    # ===============================
    job_content_path = 'job_content.py'
    info_file_path = 'info.txt'

    def send_the_job(self, path):
        """
        Send a job to job tracker
        :param path:
        :return:
        """
        mapper_content = utils.get_file_content(self.mapper.get_output_path())
        reducer_content = utils.get_file_content(self.reducer.get_output_path())
        job = Job(path, mapper_content, reducer_content)
        if self.naming_server.receive_the_job(job):
            print('Job is received successfully')
        else:
            print('No file with such path')

    def receive_the_job(self, file_path, info, job_content):
        """
        Receive a job from job tracker to execute
        :param job_content:
        :return:
        """
        # Writing an info file to info.txt
        utils.write_content(self.info_file_path, info)
        with open(self.job_content_path, mode='x') as file:
            file.write(job_content)
        print("The job is received")
        file_content = self.read(file_path)
        print("Reading the file with path " + file_path)
        self.exec_job(job_content, file_content)

    def exec_job(self, job_content, file_content):
        """
        Execute a file of a job
        :return:
        """
        exec(open(job_content).read(), file_content)

    # ===============================
    # Helpers
    # ===============================
    def get_chunks(self, content):
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
        elif 'dojob' in user_input.lower():
            path = user_input.split('(', 3)[1][:-1]
            self.send_the_job(path)


# address = sys.argv[1]
# port = int(sys.argv[2])
serv_addr = utils.get_master_address()

address = serv_addr[0]
port = serv_addr[1]
storage_id = int(sys.argv[3])

client = Slave(address, port, storage_id)

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
    client.handle_user_input(action)
