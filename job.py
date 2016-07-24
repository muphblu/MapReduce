class Job:
    output_file_path = "/job"

    def __init__(self, data, mapper, reducer):
        self.file_path = data
        self.mapper = mapper
        self.reducer = reducer

    def get_file_path(self):
        return self.file_path
