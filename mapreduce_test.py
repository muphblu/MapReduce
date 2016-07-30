import unittest
from mapreduce import Jobber


class TestMapReduce(unittest.TestCase):
    def test_getting_mapped_data(self):
        input_data = ""
        for i in range(4):
            input_data += "('string', %d)\n" % i
        tuples_list = Jobber.split_to_list(input_data)
        self.assertEqual(tuples_list, [('string', 0), ('string', 1), ('string', 2), ('string', 3)])
