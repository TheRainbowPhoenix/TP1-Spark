import unittest

from pyspark.sql import SparkSession

from src.exercises.exo_3_top5.stub import answer

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("MnMCountTest")
         .getOrCreate())


class TestTop5(unittest.TestCase):
    def expected_result(self):
        columns = ["City", "Color", "Total"]
        data = [
            ("Avignon", "Rouge", 1765),
            ("Avignon", "Bleu", 1742),
            ("Avignon", "Orange", 1722),
            ("Avignon", "Marron", 1682),
            ("Avignon", "Vert", 1641),
        ]
        expected = spark.createDataFrame(data, columns)
        return expected

    def test_transform_answer(self):
        input_file = "../../data/mnm_count.csv"
        expected_data = self.expected_result()
        actual_data = answer(input_file)
        self.assertEqual(actual_data.collect(), expected_data.collect())


if __name__ == '__main__':
    unittest.main()
