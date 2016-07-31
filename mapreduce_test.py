import unittest
from mapreduce import Jobber
from mapreduce import split_into_words


class TestMapReduce(unittest.TestCase):
    # def setUp(self):
    #     self.

    def test_getting_mapped_data(self):
        input_data = ""
        for i in range(4):
            input_data += "('string', %d)\n" % i
        tuples_list = Jobber.split_to_list(input_data)
        self.assertEqual(tuples_list, [('string', 0), ('string', 1), ('string', 2), ('string', 3)])

    def test_line_splitter(self):
        with open('test_file.txt') as file:
            string = file.read()
        resulting_list = split_into_words(string)
        self.assertEqual(['word1', 'word2', 'word3'], resulting_list)

    def test_split_into_words(self):
        string = 'word1 word2.\nword3'
        result = string.split()
        self.assertEqual(['word1', 'word2.', 'word3'], result)
