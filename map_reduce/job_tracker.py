# TODO create named tuple JobTrackerOptions for params
import utils


class JobTracker:
    def __init__(self, options):
        servers_info = utils.get_slaves_info()
        self.servers = [utils.StorageServerInfo(server[0], server[1]) for server in servers_info]
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

    def start_job(self, data, map_fun, reduce_fun):
        """
        :param data: path to the file with data to process by map/reduce
        :param map_fun: mapper function in str
        :param reduce_fun: reducer function in str
        :return:
        """
        mappers = self._get_mapper_servers()
        self.mappers_status = {mapper.id: False for mapper in mappers}
        chunks_info = self.naming_server.read(data)
        # TODO receive chunks list
        # TODO divide chunks between mappers
        chunks_count = len(chunks_info)
        # chunks_per_mapper = chunks_count/self.mappers_num
        chunks_for_mappers = [None] * self.mappers_num
        for i in range(chunks_count):
            chunks_for_mappers[chunks_count % self.mappers_num].append(chunks_info[i])

        for i in range(self.mappers_num):
            mappers[i].proxy.init_mapper(chunks_for_mappers[i], map_fun)

        self.reduce_fun = reduce_fun

    def stop_job(self):
        pass

    def man_up(self, server_id):
        pass

    def man_down(self, server_id):
        pass

    def map_finished(self, server_id):
        """Notification that map finished on particular server"""
        self.mappers_status[server_id] = True
        for mapper in self.mappers.items():
            if not mapper[1]:
                return
        self._start_reducers()

    # ==========================================
    # Private
    # ==========================================
    def _start_reducers(self):
        reducers = self._get_reducer_servers
        for reducer in reducers:
            reducer.proxy.init_reducer(self.mappers_status.keys())

    def _get_mapper_servers(self):
        return self.servers[:self.mappers_num]

    def _get_reducer_servers(self):
        return self.servers[:self.mappers_num + self.reducers_num]
