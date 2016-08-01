import os
import shutil
import time
import sys
import utils
from map_reduce.job_tracker import JobTracker
from naming_server import NamingServer
from xmlrpc.server import SimpleXMLRPCServer


FILES_ROOT = 'files/'
NAMING_REPOSITORY_ROOT = FILES_ROOT + 'filesystem/'


class Master:
    def __init__(self):
        # Init master files structure
        self.init_master_files_structure()

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
        self.server.register_function(self.job_tracker.reduce_finished)
        self.server.register_function(self.job_tracker.check_job_status)
        self.server.register_function(self.job_tracker.get_results)

        self.start()

    @staticmethod
    def init_master_files_structure():
        if not os.path.isdir(FILES_ROOT):
            sys.exit('No root directory [' + FILES_ROOT + '], please create it first')

        # reset root filesystem directory
        if os.path.isdir(NAMING_REPOSITORY_ROOT):
            shutil.rmtree(NAMING_REPOSITORY_ROOT)
        time.sleep(1)
        os.mkdir(NAMING_REPOSITORY_ROOT)

    def start(self):
        # Starting RPC server(should be last)
        self.server.serve_forever()


master = Master()
