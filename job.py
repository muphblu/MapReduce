class Job:
    output_file_path = "/job"

    def __init__(self, data, mapper, reducer):
        self.data = data
        self.mapper = mapper
        self.reducer = reducer
