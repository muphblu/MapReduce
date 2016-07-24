class Mapper:
    output_file_path = "/mapper"

    def __init__(self, file, separator='\t'):
        self.mapper = ''
        self.file = file
        self.separator = separator

    def get_output_path(self):
        return self.output_file_path

    # TODO Implement
    # def map(self):
