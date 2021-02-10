import unittest

from src.exercises.exo_2_filter_by_rating.stub import answer


class TestFilterByRating(unittest.TestCase):
    def expected_result(self):
        expected = "Homeward Bound: The Incredible Journey"
        return expected

    def test_transform_answer(self):
        expected_data = self.expected_result()
        actual_data = answer("../../data/netflix_shows.csv")
        self.assertEqual(expected_data, actual_data)


if __name__ == '__main__':
    unittest.main()
