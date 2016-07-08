class Client:
    def __init__(self, port):
        s = xmlrpclib.ServerProxy('http://localhost:' + port)
        self.server.register_function(self.read)
        self.server.register_function(self.write)
        self.server.register_function(self.createFile)
        self.server.register_function(self.deleteFile)
        self.server.register_function(self.createDirectory)
        self.server.register_function(self.deleteDirectory)

    def read(self, path):
        """
        Read file from storage servers through path received by Naming Server
        :param path: Path in FS from where to read
        """
        pass

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
