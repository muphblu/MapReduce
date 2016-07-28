import os

import utils

MAPPER_CONTENT_PATH = os.path.abspath(os.path.dirname(__file__)) + '\mapper.py'
REDUCER_CONTENT_PATH = os.path.abspath(os.path.dirname(__file__)) + r'\reducer.py'


def get_map_code():
    return utils.get_file_content(MAPPER_CONTENT_PATH)


def get_reducer_code():
    return utils.get_file_content(REDUCER_CONTENT_PATH)
