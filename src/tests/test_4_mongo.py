import unittest

from pyspark.sql import Row

from src.exercises.exo_4_mongodb.stub import answer


class TestByAge(unittest.TestCase):

    def expected_result(self):
        expected = [
            Row(name='Oin', age=167),
            Row(name='Dwalin', age=169),
            Row(name='Balin', age=178),
            Row(name='Thorin', age=195),
            Row(name='Gloin', age=158)
        ]
        return sorted(expected, key=(lambda row: row.name))

    def test_transform_answer(self):
        expected_data = self.expected_result()
        actual_data = sorted(answer(), key=(lambda row: row.name))
        self.assertEqual(actual_data, expected_data)


if __name__ == '__main__':
    unittest.main()
