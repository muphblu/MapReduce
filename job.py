class Job:
    def __init__(self, data, mapper_content, reducer_content):
        self.data = data
        self.mapper_content = mapper_content
        self.reducer_content = reducer_content
