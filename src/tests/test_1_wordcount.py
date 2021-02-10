import unittest

from src.exercises.exo_1_wordcount.stub import answer


class TestParallelize(unittest.TestCase):
    def expected_result(self):
        expected = ('de', 2062)
        return expected

    def test_transform_answer(self):
        expected_data = self.expected_result()
        actual_data = answer("../../data/lupin.txt")
        self.assertEqual(expected_data, actual_data)


if __name__ == '__main__':
    unittest.main()
