# TODO create named tuple JobTrackerOptions for params
import utils


class JobTracker:
    def __init__(self, options):
        servers_info = utils.get_slaves_info()
        self.servers = [utils.StorageServerInfo(server[0], server[1]) for server in servers_info]
        pass

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

        pass

    def stop_job(self):
        pass

    def man_up(self, server_id):
        pass

    def man_down(self, server_id):
        pass
