import unittest

import pyspark

from src.exercises.exo_0_parallelize.stub import answer

spark = pyspark.SparkContext.getOrCreate()


class TestParallelize(unittest.TestCase):
    def expected_result(self):
        return spark.parallelize([1, 2, 3, 4])

    def test_transform_answer(self):
        expected_data = self.expected_result()
        actual_data = answer()
        self.assertEqual(actual_data.collect(), expected_data.collect())


if __name__ == '__main__':
    unittest.main()
