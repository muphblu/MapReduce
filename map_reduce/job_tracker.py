# TODO create named tuple JobTrackerOptions for params
import os

import shutil

import utils


class JobTracker:
    def __init__(self):
        self.results = []
        slaves_info = utils.get_slaves_info()
        self.slaves = [utils.StorageServerInfo(server[0], server[1]) for server in slaves_info]
        config = utils.get_configuration()
        self.mappers_num = config['mappers_num']
        self.reducers_num = config['reducers_num']
        self.is_job_finished = False

    # ==========================================
    # API
    # ==========================================
    def init_naming_server(self, naming_server):
        self.naming_server = naming_server

    # ==========================================
    # RPC API
    # ==========================================
    def startup(self):
        pass

    def shutdown(self):
        pass

    def start_job(self, data_path, map_function, reduce_function):
        """
        Starts job
        :param data_path: path to the file with data to process by map/reduce
        :param map_function: mapper function in str
        :param reduce_function: reducer function in str
        """
        if os.path.exists("files/filesystem/results"):
            shutil.rmtree("files/filesystem/results")
        os.mkdir("files/filesystem/results")

        self.is_job_finished = False
        self.results.clear()
        self.reduce_fun = reduce_function

        mappers = self._get_mapper_servers()
        self.mappers_status = {mapper.id: False for mapper in mappers}

        chunks_info = self.naming_server.read(data_path)
        chunks_count = len(chunks_info)
        # chunks_per_mapper = chunks_count/self.mappers_num
        chunks_for_mappers = [[]] * self.mappers_num
        for i in range(chunks_count):
            chunks_for_mappers[chunks_count % self.mappers_num].append(chunks_info[i])

        reducers_ids = [x.id for x in self._get_reducer_servers()]
        for i in range(self.mappers_num):
            mappers[i].proxy.init_mapper(chunks_for_mappers[i], map_function, reducers_ids)

    def stop_job(self):
        pass

    def man_up(self, server_id):
        """
        Notification that server is up after being down
        :param server_id: server that turned on
        """
        pass

    def man_down(self, server_id):
        """
        Notification that server is went down
        :param server_id: server that went down
        """
        pass

    def map_finished(self, server_id):
        """
        Notification that map finished on particular server
        Runs reducers when all mappers finished the work
        :param server_id: mapper that finished the work
        """
        self.mappers_status[server_id] = True
        for key, value in self.mappers_status.items():
            if not value:
                return
        self._start_reducers()

    def reduce_finished(self, server_id, reduced_file_path):
        """
        Notification from reducer that it finished his work
        :param server_id: server id of reducer
        :param reduced_file_path: result file path in DFS
        """
        self.results.append(reduced_file_path)
        self.reducers_status[server_id] = True
        for reducer in self.reducers_status.items():
            if not reducer[1]:
                return
        self._process_results()
        pass

    # ==========================================
    # Private
    # ==========================================
    def _start_reducers(self):
        """
        Starts reducers
        """
        reducers = self._get_reducer_servers()
        self.reducers_status = {reducer.id: False for reducer in reducers}
        for reducer in reducers:
            reducer.proxy.init_reducer(list(self.mappers_status.keys()))

    def _get_mapper_servers(self):
        """
        :return: List of servers which should be mappers
        """
        return self.slaves[:self.mappers_num]

    def _get_reducer_servers(self):
        """
        :return: List of servers which should be reducers
        """
        return self.slaves[self.mappers_num:self.mappers_num + self.reducers_num]

    def _process_results(self):
        self.is_job_finished = True
        pass

    def check_job_status(self):
        """Return status of the job"""
        return self.is_job_finished

    def get_results(self):
        """Returns list of result files paths in dfs"""
        if self.is_job_finished:
            return self.results
        return None
