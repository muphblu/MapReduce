#!/usr/bin/env python

import os
import shutil
import uuid
from threading import Thread

import time

import utils
from utils import FileInfo, ChunkInfo, DirFileEnum, StorageServerInfo
from xmlrpc.server import SimpleXMLRPCServer


class NamingServer:
    def __init__(self):
        """NamingServer"""
        self.repository_root = 'filesystem/'

        # Naming server configuration
        address_str = utils.get_own_address()
        address = address_str.split(":")
        # Connection to storage servers
        self.storage_servers = [StorageServerInfo(server_info[0], server_info[1]) for server_info in
                                utils.get_servers_info()]

        # reset root filesystem directory
        if os.path.isdir(self.repository_root):
            shutil.rmtree(self.repository_root)
        time.sleep(1)
        os.mkdir(self.repository_root)

        self.server = SimpleXMLRPCServer(('localhost', int(address[1])), logRequests=False, allow_none=True)

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
        self.server.register_function(self.do_the_job)

        Thread(target=self.ping_echo_loop).start()
        # Starting RPC server(should be last)
        self.server.serve_forever()

    # ===============================
    # NamingServer API
    # ===============================
    def read(self, path):
        """
        Read file from DFS
        :param path: Path in FS from where to read
        :return: ordered list of named tuples type of utils.ChunkInfo
        """
        total_path = self.repository_root + path
        return self.serialize_chunk_info(FileInfo.get_file_info(total_path).chunks)

    def write(self, path, size, count_chunks):
        """
        Writes file to DFS
        :param path: path to file
        :param size: file size in
        :param count_chunks: Number of chunks to write
        :return: ordered list of named tuples type of utils.ChunkInfo
        """
        total_path = self.repository_root + path
        chunk_info_list = [self.generate_chunk_info(chunk_number) for chunk_number in
                           range(0, count_chunks)]

        FileInfo(total_path, size, chunk_info_list).save_file()
        self.add_server_file_info(chunk_info_list, total_path)

        return self.serialize_chunk_info(chunk_info_list)

    def delete(self, path):
        """
        Deletes file
        :param path: path to file to delete
        :return: ?
        """
        # TODO think whether we should return something or client should handle exceptions thrown here
        total_path = self.repository_root + path
        if os.path.isfile(total_path):
            file_info = FileInfo.get_file_info(total_path)
            for chunk in file_info.chunks:
                # TODO maybe add 'try' here to delete info from NS even if SSs are down
                for server in list(filter(
                        lambda serv: serv.id == chunk.main_server_id or serv.id == chunk.replica_server_id,
                        self.storage_servers)):
                    server.proxy.delete(chunk.chunk_name)

            self.delete_server_file_info(total_path)
            os.remove(total_path)
            result = 'Removed file'
            print(result)
        elif os.path.isdir(total_path):
            os.rmdir(total_path)
            result = 'Removed directory'
            print(result)
        else:
            result = 'Neither file nor directory. Or does not exist'
            print(result)
        return result

    def size(self, path):
        """
        Provides size of file in DFS
        :param path: path to file
        :return: size
        """
        total_path = self.repository_root + path
        file_info = FileInfo.get_file_info(total_path)
        return file_info.size

    def list(self, path):
        """
        Lists all files and directories in current directory
        :param path: path to directory
        :return: list of strings with names of files and directories in path
        """
        dir_path = self.repository_root + path
        try:
            return os.listdir(dir_path)
        except FileNotFoundError:
            result = 'Given path not found'
            print(result)
            return result
        except NotADirectoryError:
            result = 'given path is not a directory'
            print(result)
            return result

    def mkdir(self, path):
        # TODO: think about return type
        """
        Creates directory
        :param path: path to new directory
        :return: result of operation
        """
        # check here the correctness of the path
        dir_path = self.repository_root + path
        print(dir_path)  # debug print
        try:
            os.makedirs(dir_path)
            result = 'success'
        except FileExistsError:
            result = 'given path is not a directory'
            print(result)
        return result

    def rmdir(self, path):
        # TODO: think about return type
        """
        Removes directory
        :param path: path to directory to remove
        :return: result of operation
        """
        dir_path = self.repository_root + path
        try:
            os.rmdir(dir_path)
            result = 'success'
        except NotADirectoryError:
            result = 'given path is not a directory'
            print(result)
        except OSError:
            result = 'directory is not empty'
            print(result)
        return result

    def get_type(self, path):
        """
        Get type of object
        :param path: path to object
        :return: utils.DirFileEnum
        """
        total_path = self.repository_root + path
        if os.path.isfile(total_path):
            # print('It is file')
            return DirFileEnum.File
        elif os.path.isdir(total_path):
            # print('It is directory')
            return DirFileEnum.Directory
        else:
            # print('Neither file nor directory')
            return DirFileEnum.Error

    def get_storages_info(self):
        """
        Provides list of storage servers addresses for the client
        :return: list with tuples where first param is server id and second is server network address "127.0.0.1:8000"
        """
        return utils.get_servers_info()

    # ===============================
    # Job tracker
    # ===============================
    # TODO Implement
    def do_the_job(self, job):
        job = job

    # ===============================
    # Helpers
    # ===============================

    def serialize_chunk_info(self, chunk_info_list):
        return list(map(lambda x: dict(x._asdict()), chunk_info_list))

    def generate_chunk_info(self, number_of_chunk):
        """
        Generates ChunkInfo objects for currently available servers and amount of chunks
        :param number_of_chunk: amount of chunks
        :return: list of utils.ChunkInfo objects
        """
        server_ids = list(map(lambda x: x.id, self.storage_servers))
        servers_number = len(server_ids)
        main_index = number_of_chunk % servers_number
        replication_index = (number_of_chunk + 1) % servers_number
        return ChunkInfo(number_of_chunk, str(uuid.uuid4()), server_ids[main_index], server_ids[replication_index])

    def add_server_file_info(self, chunk_info_list, path):
        # TODO create similar method to delete file
        """
        Adds info to the servers from self.storage_servers, about added file
        :param chunk_info_list: list of ChunkInfo objects for this file
        :param path: path to file
        :return: ?
        """
        for chunk_info in chunk_info_list:
            for server in list(filter(
                    lambda server: server.id == chunk_info.main_server_id or server.id == chunk_info.replica_server_id,
                    self.storage_servers)):
                server.files.add(path)

    def delete_server_file_info(self, path):
        """
        Deletes info about file from self.storage_servers
        :param path:
        :return:
        """
        for server in self.storage_servers:
            if path in server.files:
                server.files.remove(path)

    def get_storage_server_by_id(self, server_id):
        for server in self.storage_servers:
            if server.id == server_id:
                return server

    # ===============================
    # Replication
    # ===============================

    def ping_echo_loop(self):
        while True:
            time.sleep(3)
            for server in self.storage_servers:
                try:
                    if not server.proxy.ping():
                        self.replicate_from_server(server.id)
                except:
                    self.replicate_from_server(server.id)

    def replicate_from_server(self, server_id):
        print('replicating ', server_id)
        # TODO use queue to put replication tasks and separate thread that will perform replication
        server = self.get_storage_server_by_id(server_id)
        for file in server.files:
            file_info = FileInfo.get_file_info(file)
            new_chunks = []
            for chunk in file_info.chunks:
                print(chunk)
                if chunk.main_server_id == server_id:
                    new_server = (server_id + 1) % len(utils.get_servers_info()) + 1
                    while True:
                        # new_server = (new_server + 1) % len(utils.get_servers_info())
                        if new_server == chunk.replica_server_id:
                            new_server = (new_server + 1) % len(utils.get_servers_info()) + 1
                            continue
                        else:
                            break
                    try:
                        print('new server ', new_server)
                        self.get_storage_server_by_id(chunk.replica_server_id).proxy.replicate(
                            chunk.chunk_name, new_server)
                        new_chink = ChunkInfo(chunk.chunk_position, chunk.chunk_name, new_server,
                                              chunk.replica_server_id)
                        new_chunks.append(new_chink)
                    except:
                        print("Error replication 1")
                        pass
                elif chunk.replica_server_id == server_id:
                    new_server = (server_id + 1) % len(utils.get_servers_info()) + 1
                    while True:
                        if new_server == chunk.main_server_id:
                            new_server = (new_server + 1) % len(utils.get_servers_info()) + 1
                            continue
                        else:
                            break
                    try:
                        print('new server ', new_server)
                        self.get_storage_server_by_id(chunk.main_server_id).proxy.replicate(
                            chunk.chunk_name, new_server)
                        new_chink = ChunkInfo(chunk.chunk_position, chunk.chunk_name, chunk.main_server_id,
                                              new_server)
                        new_chunks.append(new_chink)
                    except:
                        print("Error replication 2")
                        pass
                else:
                    new_chunks.append(chunk)
            file_info.chunks = new_chunks
            file_info.save_file()
            self.add_server_file_info(new_chunks, file)
        # Assume that broken server is not containing files anymore
        server.files.clear()

    def main(self, argv):
        # self.mkdir('/r')
        # self.rmdir('/r')
        # self.list('/``')
        # self.get_type('/r/file')
        # self.delete('/r/file')
        pass


s = NamingServer()
# s.write("lol.txt", 1024, 2)
# s.delete("lol.txt")

# if __name__ == '__main__':
#     server = NamingServer()
#     # testing here
#     server.main('a')  # passing parameter that is not needed for now
