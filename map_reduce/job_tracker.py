# TODO create named tuple JobTrackerOptions for params


class JobTracker:
    def __init__(self, options):
        pass

    def startup(self):
        pass

    def shutdown(self):
        pass

    def start_job(self, data, map_fun, reduce_fun):
        pass

    def stop_job(self):
        pass

    def man_up(self, server_id):
        pass

    def man_down(self, server_id):
        pass
