from xmlrpc.server import SimpleXMLRPCServer


class StorageServer:
    def __init__(self):
        self.server = SimpleXMLRPCServer(("localhost", 8000))
        self.server.register_function(self.read, "read")
        self.server.register_function(self.write, "write")
        self.server.register_function(self.delete, "delete")
        # self.server.register_function(self.size, "size")
        self.server.serve_forever()
        pass

    def read(self):
        return "return from storage.read()"
        pass

    def write(self):
        return "return from storage.read()"

    def delete(self):
        return "return from storage.read()"

    # def size(self):
    #     return "return from storage.read()"
