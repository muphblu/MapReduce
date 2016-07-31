from xmlrpc.server import SimpleXMLRPCServer

import utils
from map_reduce.job_tracker import JobTracker
from naming_server import NamingServer


class Master:
    def __init__(self):
        self.naming_server = NamingServer()
        self.job_tracker = JobTracker()
        # Pass links on objects
        self.job_tracker.init_naming_server(self.naming_server)
        self.naming_server.init_job_tracker(self.job_tracker)

        self.address = utils.get_master_address()
        self.server = SimpleXMLRPCServer(self.address, logRequests=False, allow_none=True)

        # registering functions
        self.server.register_function(self.naming_server.read)
        self.server.register_function(self.naming_server.write)
        self.server.register_function(self.naming_server.delete)
        self.server.register_function(self.naming_server.size)
        self.server.register_function(self.naming_server.list)
        self.server.register_function(self.naming_server.mkdir)
        self.server.register_function(self.naming_server.rmdir)
        self.server.register_function(self.naming_server.get_type)
        self.server.register_function(self.naming_server.get_storages_info)

        self.server.register_function(self.job_tracker.startup)
        self.server.register_function(self.job_tracker.shutdown)
        self.server.register_function(self.job_tracker.start_job)
        self.server.register_function(self.job_tracker.stop_job)
        self.server.register_function(self.job_tracker.man_up)
        self.server.register_function(self.job_tracker.man_down)
        self.server.register_function(self.job_tracker.map_finished)

    def start(self):
        # Starting RPC server(should be last)
        self.server.serve_forever()


master = Master()
