# TODO create named tuple JobTrackerOptions for params
import utils


class JobTracker:
    def __init__(self):
        slaves_info = utils.get_slaves_info()
        self.slaves = [utils.StorageServerInfo(server[0], server[1]) for server in slaves_info]
        config = utils.get_configuration()
        self.mappers_num = config['mappers_num']
        self.reducers_num = config['reducers_num']
        pass

    # ==========================================
    # API
    # ==========================================
    def init_naming_server(self, naming_server):
        self.naming_server = naming_server;

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
        for mapper in self.mappers_status.items():
            if not mapper[1]:
                return
        self._start_reducers()

    # ==========================================
    # Private
    # ==========================================
    def _start_reducers(self):
        """
        Starts reducers
        """
        reducers = self._get_reducer_servers()
        for reducer in reducers:
            reducer.proxy.init_reducer(self.mappers_status.keys())

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
